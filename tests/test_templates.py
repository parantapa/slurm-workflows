"""Tests for the Jinja templates, and for their loader.

The loader reads several templates from one file.
"""

from __future__ import annotations

import json
import os
import time
import signal
import subprocess
from pathlib import Path

import jinja2
import pytest

from slurm_workflows.templates import (
    line_col_from_pos,
    parse_file,
    render_template,
    shell_quote,
)


def run_sbatch_script(
    script: str, tmp_path: Path, **env: str
) -> subprocess.CompletedProcess[str]:
    """Run a rendered sbatch body against a stub `srun`, and return both streams.

    The script sees only `PATH`, with the stub's directory first, and `env`.
    The stub echoes its command line to stdout,
    mixed in with what the script itself echoes.
    It raises `subprocess.CalledProcessError` when the script exits non-zero.
    """

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    srun = bin_dir / "srun"
    srun.write_text('#!/bin/bash\necho "srun $*"\n')
    srun.chmod(0o755)

    script_path = tmp_path / "job.sbatch"
    script_path.write_text(script)

    # Only the shell knows which `srun` runs.
    # See the comment above `num_slurm_tasks=` in `templates/slurm_pilot.jinja`.
    # The script inherits only `PATH`,
    # so the SLURM variables are exactly what a case sets.
    # This holds even when the suite itself runs from inside a Slurm job.
    proc = subprocess.run(
        ["bash", str(script_path)],
        capture_output=True,
        text=True,
        check=True,
        env={"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", **env},
    )
    return proc


class TestParseFile:
    """The format of a file that holds several templates.

    A file is a run of `{#- <json5 header> -#}` markers,
    each followed by the body of the template it names.
    Nothing else in the file is addressable,
    which is what the cases in this class pin down.
    """

    def write(self, tmp_path: Path, text: str) -> Path:
        path = tmp_path / "sample.jinja"
        path.write_text(text)
        return path

    def test_reads_every_template_in_the_file(self, tmp_path: Path):
        path = self.write(
            tmp_path,
            '{#- name: "first" -#}\nbody one\n{#- name: "second" -#}\nbody two\n',
        )

        templates = parse_file("sample", path)

        assert sorted(templates) == ["sample:first", "sample:second"]
        assert templates["sample:first"].source == "body one"
        assert templates["sample:second"].source == "body two"

    def test_a_body_is_stripped(self, tmp_path: Path):
        path = self.write(tmp_path, '{#- name: "only" -#}\n\n\n  body  \n\n')

        assert parse_file("sample", path)["sample:only"].source == "body"

    def test_an_unterminated_header_is_reported(self, tmp_path: Path):
        path = self.write(tmp_path, '{#- name: "only"\nbody with no header end\n')

        with pytest.raises(ValueError, match="Unable to find end of header"):
            parse_file("sample", path)

    def test_a_malformed_header_is_reported(self, tmp_path: Path):
        path = self.write(tmp_path, "{#- name: -#}\nbody\n")

        with pytest.raises(Exception) as excinfo:
            parse_file("sample", path)

        assert "Failed to parse template file" in "".join(excinfo.value.__notes__)

    def test_the_error_carries_a_file_position(self, tmp_path: Path):
        path = self.write(tmp_path, '{#- name: "ok" -#}\nbody\n{#- oops -#}\n')

        with pytest.raises(Exception) as excinfo:
            parse_file("sample", path)

        notes = "".join(excinfo.value.__notes__)
        assert str(path) in notes

    def test_a_jinja_comment_in_a_body_is_read_as_the_next_header(self, tmp_path: Path):
        """A limit of the format, not a bug to correct by accident.

        See the developer notes, "Templates".
        """
        path = self.write(
            tmp_path,
            '{#- name: "only" -#}\nbody\n{#- a note -#}\nmore body\n',
        )

        with pytest.raises(Exception):
            parse_file("sample", path)


class TestLineColFromPos:

    def test_counts_lines_and_columns(self):
        text = "one\ntwo\nthree"

        assert line_col_from_pos(text, text.index("two")) == (2, 1)
        assert line_col_from_pos(text, text.index("three") + 2) == (3, 3)

    def test_an_empty_text_is_the_start(self):
        assert line_col_from_pos("", 0) == (1, 1)


class TestLoader:
    # Each call here is one that no `render_template` overload accepts.
    # The `pyright: ignore[reportCallIssue]` pragmas mark those calls as deliberate.

    def test_unknown_file_prefix_raises(self):
        with pytest.raises(jinja2.TemplateNotFound):
            render_template("no_such_file:whatever")  # pyright: ignore[reportCallIssue]

    def test_missing_variable_is_a_hard_error(self):
        """The environment uses `StrictUndefined`."""
        with pytest.raises(jinja2.UndefinedError):
            render_template(
                "slurm_pilot:worker_sbatch_script",  # pyright: ignore[reportCallIssue]
                is_batch_worker=True,
                # The call omits `worker_script_path` on purpose.
            )


class TestWorkerSbatchScript:
    def render(
        self,
        name: str = "testex.job.cpu.0",
        work_dir: str = "/scratch/work",
        is_batch_worker: bool = False,
        worker_script_path: str = "/path/to/worker.sh",
    ) -> str:
        # This method spells out its arguments
        # rather than gather them into `**overrides`.
        # `render_template` is a set of `@overload`s keyed on the template name.
        # A `**kwargs` dict erases the per-argument types that the overloads match on.
        return render_template(
            "slurm_pilot:worker_sbatch_script",
            name=name,
            work_dir=work_dir,
            is_batch_worker=is_batch_worker,
            worker_script_path=worker_script_path,
        )

    def test_batch_worker_sources_script_directly(self):
        out = self.render(is_batch_worker=True)

        assert ". '/path/to/worker.sh'" in out
        assert "srun" not in out

    def test_the_output_pattern_lands_in_the_work_dir(self):
        out = self.render(work_dir="/some/other/dir")

        assert "--output '/some/other/dir/" in out


class TestPilotJobEvents:
    """The batch script reports its own start and exit through the worker script.

    These run the shell against two stubs.
    The stub worker script echoes its arguments.
    The stub `srun` echoes its command line,
    or sleeps until the test signals the job.
    """

    def render(self, tmp_path: Path, is_batch_worker: bool) -> str:
        worker_script = tmp_path / "worker.sh"
        worker_script.write_text('echo "worker $*"\n')
        return render_template(
            "slurm_pilot:worker_sbatch_script",
            name="testex.job.cpu.0",
            work_dir="/scratch/work",
            is_batch_worker=is_batch_worker,
            worker_script_path=worker_script,
        )

    @pytest.mark.parametrize("is_batch_worker", [False, True])
    def test_start_and_exit_bracket_the_worker(
        self, tmp_path: Path, is_batch_worker: bool
    ):
        out = run_sbatch_script(
            self.render(tmp_path, is_batch_worker), tmp_path, SLURM_NTASKS="1"
        ).stdout

        reported = [ln for ln in out.splitlines() if "--pilot-job-event" in ln]
        assert reported == [
            "worker --pilot-job-event start",
            "worker --pilot-job-event exit",
        ]

    def test_a_failed_report_does_not_end_the_job(self, tmp_path: Path, srun_lines):
        worker_script = tmp_path / "worker.sh"
        worker_script.write_text('[[ "$*" != *pilot-job-event* ]] || exit 1\n')
        script = render_template(
            "slurm_pilot:worker_sbatch_script",
            name="testex.job.cpu.0",
            work_dir="/scratch/work",
            is_batch_worker=False,
            worker_script_path=worker_script,
        )

        proc = run_sbatch_script(script, tmp_path, SLURM_NTASKS="1")

        assert proc.returncode == 0
        assert len(srun_lines(proc.stdout)) == 1

    def test_sigterm_still_reports_the_exit(self, tmp_path: Path):
        """What Slurm does when it cancels the job or the time limit passes."""
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        srun = bin_dir / "srun"
        srun.write_text('#!/bin/bash\ntouch "$STARTED"\nexec sleep 30\n')
        srun.chmod(0o755)
        script_path = tmp_path / "job.sbatch"
        script_path.write_text(self.render(tmp_path, is_batch_worker=False))
        started = tmp_path / "started"

        proc = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            # A group of its own, since Slurm signals every process of the job.
            start_new_session=True,
            env={
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "SLURM_NTASKS": "1",
                "STARTED": str(started),
            },
        )
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        os.killpg(proc.pid, signal.SIGTERM)
        out, _ = proc.communicate(timeout=10)

        # 143 is 128 + SIGTERM, what the TERM trap exits with.
        assert proc.returncode == 143
        assert out.splitlines()[-1] == "worker --pilot-job-event exit"


class TestOutputRedirectByTaskCount:
    """Which `srun` a job runs. These cases run the shell to find out.

    The rule they pin is in the comment above `num_slurm_tasks=`
    in `templates/slurm_pilot.jinja`.
    Anything that is not a Slurm job at all keeps the per-task files.

    Each case names the `sbatch` options it stands for,
    and sets the variables Slurm sets for them.
    """

    def render(self) -> str:
        return render_template(
            "slurm_pilot:worker_sbatch_script",
            name="testex.job.cpu.0",
            work_dir="/scratch/work",
            is_batch_worker=False,
            worker_script_path="/path/to/worker.sh",
        )

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param(
                {"SLURM_NTASKS": "1", "SLURM_JOB_NUM_NODES": "1"},
                id="ntasks-1",
            ),
            pytest.param(
                {"SLURM_NTASKS": "1", "SLURM_JOB_NUM_NODES": "2"},
                id="ntasks-1-over-two-nodes",
            ),
            pytest.param(
                {"SLURM_JOB_NUM_NODES": "1"},
                id="nodes-1-with-no-task-option",
            ),
        ],
    )
    def test_a_single_task_job_writes_to_the_batch_file(
        self, tmp_path: Path, env: dict[str, str], srun_lines
    ):
        # The last case names no task count, so `SLURM_NTASKS` is unset.
        out = run_sbatch_script(self.render(), tmp_path, **env).stdout

        # One line, because only one of the two branches can run.
        assert srun_lines(out) == ["srun /bin/bash /path/to/worker.sh"]

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param(
                {"SLURM_NTASKS": "8", "SLURM_JOB_NUM_NODES": "4"},
                id="four-nodes-two-tasks-each",
            ),
            pytest.param(
                {"SLURM_NTASKS": "4", "SLURM_JOB_NUM_NODES": "4"},
                id="four-nodes-one-task-each",
            ),
            pytest.param(
                {"SLURM_NTASKS": "4", "SLURM_JOB_NUM_NODES": "1"},
                id="one-node-four-tasks",
            ),
            pytest.param(
                {"SLURM_JOB_NUM_NODES": "4"},
                id="four-nodes-with-no-task-option",
            ),
            pytest.param({}, id="not-a-slurm-job"),
        ],
    )
    def test_every_other_job_keeps_its_per_task_files(
        self, tmp_path: Path, env: dict[str, str], srun_lines
    ):
        # One task *per node* is not one task.
        out = run_sbatch_script(self.render(), tmp_path, **env).stdout

        assert srun_lines(out) == [
            "srun --output /scratch/work/testex.job.cpu.0-%j-%t.out "
            "/bin/bash /path/to/worker.sh"
        ]

    def test_the_count_it_decided_on_is_echoed(self, tmp_path: Path):
        """The batch output file records the task count the shell decided on."""
        proc = run_sbatch_script(
            self.render(),
            tmp_path,
            SLURM_NTASKS="4",
            SLURM_JOB_NUM_NODES="1",
        )

        assert "Num Slurm tasks: 4" in proc.stdout

    @pytest.mark.parametrize(
        "env, traced",
        [
            pytest.param({"SLURM_NTASKS": "1"}, "+ srun /bin/bash", id="single-task"),
            pytest.param({"SLURM_NTASKS": "4"}, "+ srun --output", id="several-tasks"),
        ],
    )
    def test_the_srun_command_is_traced(
        self, tmp_path: Path, env: dict[str, str], traced: str
    ):
        """`set -x` traces the `srun` command line, in either branch."""
        proc = run_sbatch_script(self.render(), tmp_path, **env)

        assert traced in proc.stderr

    def test_the_guard_survives_a_strict_shell(self, tmp_path: Path, srun_lines):
        """The script reads both variables with `:-`, so `set -u` does not break it."""
        out = run_sbatch_script("set -u\n" + self.render(), tmp_path).stdout

        (command,) = srun_lines(out)
        assert "--output" in command


class TestWorkerScript:
    def render(
        self,
        setup_script: str = "module load gcc\nconda activate my-env",
        actor_class_name: str = "",
    ) -> str:
        # This method spells out its arguments
        # for the same reason as `TestWorkerSbatchScript.render` does.
        return render_template(
            "slurm_pilot:worker_script",
            worker_exe="slurm-pilot-worker",
            setup_script=setup_script,
            group="cpu",
            name="testex.job.cpu.0",
            actor_class_name=actor_class_name,
            server_address="10.0.0.1:5051",
            work_dir="/scratch/work",
            python_paths_json=json.dumps(["/a", "/b"]),
            # Any value will do. See `TestWorkerRestart.RESTART_EXIT_CODE`.
            restart_exit_code=75,
        )

    def test_multiline_body_is_not_escaped(self):
        out = self.render(setup_script='export A="x y"\nexport B=$HOME/z\n# a comment')

        assert 'export A="x y"' in out
        assert "export B=$HOME/z" in out
        assert "# a comment" in out

    def test_empty_body_is_allowed(self):
        out = self.render(setup_script="")

        assert ". '/etc/profile'" in out
        assert "slurm-pilot-worker \\" in out

    def test_passes_all_worker_arguments(self):
        out = self.render(actor_class_name="pkg.mod.MyActor")

        assert "--group 'cpu'" in out
        assert "--actor-class-name 'pkg.mod.MyActor'" in out
        assert "--name 'testex.job.cpu.0'" in out
        assert "--server-address '10.0.0.1:5051'" in out
        assert "--work-dir '/scratch/work'" in out
        assert """--python-paths-json '["/a", "/b"]'""" in out


class TestWorkerRestart:
    """The worker script starts the worker again when the worker exits for a restart."""

    # The template takes the code as a variable, so these tests pass their own.
    # The executor passes `slurm_pilot_worker.RESTART_EXIT_CODE`.
    RESTART_EXIT_CODE = 75

    # These tests run the rendered script with bash against this stub worker.
    # For each run, the stub appends its arguments as a line to `$RUNS`.
    # Then the stub exits with the status
    # that `STUB_STATUSES` lists for that run,
    # or 0 once the list runs out.
    # The stub appends a pilot job event to `$EVENTS` instead,
    # and always exits 0 for it.
    STUB = """#!/bin/bash
if [[ "$*" == *--pilot-job-event* ]] ; then
    echo "$*" >> "$EVENTS"
    exit 0
fi
echo "$*" >> "$RUNS"
runs=$(wc -l < "$RUNS")
read -r -a statuses <<< "$STUB_STATUSES"
exit "${statuses[$((runs - 1))]:-0}"
"""

    def write_worker_script(self, tmp_path: Path) -> Path:
        stub = tmp_path / "stub-worker"
        stub.write_text(self.STUB)
        stub.chmod(0o755)
        script = render_template(
            "slurm_pilot:worker_script",
            worker_exe=str(stub),
            setup_script='echo setup >> "$SETUP_LOG"',
            group="cpu",
            name="testex.job.cpu.0",
            actor_class_name="",
            server_address="10.0.0.1:5051",
            work_dir="/scratch/work",
            python_paths_json=json.dumps(["/a"]),
            restart_exit_code=self.RESTART_EXIT_CODE,
        )
        script_path = tmp_path / "worker.sh"
        script_path.write_text(script)
        return script_path

    def env(self, tmp_path: Path, statuses: str) -> dict[str, str]:
        return {
            "RUNS": str(tmp_path / "runs"),
            "EVENTS": str(tmp_path / "events"),
            "SETUP_LOG": str(tmp_path / "setup"),
            "STUB_STATUSES": statuses,
        }

    def run(
        self, tmp_path: Path, statuses: str, *args: str
    ) -> subprocess.CompletedProcess[str]:
        """Run the worker script by itself, as `srun` does."""
        return subprocess.run(
            ["bash", str(self.write_worker_script(tmp_path)), *args],
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **self.env(tmp_path, statuses)},
        )

    def lines(self, path: Path) -> list[str]:
        return path.read_text().splitlines() if path.exists() else []

    def test_a_restart_status_starts_the_worker_again(self, tmp_path: Path):
        proc = self.run(tmp_path, f"{self.RESTART_EXIT_CODE} 0")

        assert proc.returncode == 0
        assert len(self.lines(tmp_path / "runs")) == 2
        assert "Worker asked for a restart" in proc.stdout

    def test_it_restarts_as_often_as_the_worker_asks(self, tmp_path: Path):
        code = self.RESTART_EXIT_CODE
        proc = self.run(tmp_path, f"{code} {code} {code} 0")

        assert proc.returncode == 0
        assert len(self.lines(tmp_path / "runs")) == 4

    def test_each_run_gets_the_same_arguments(self, tmp_path: Path):
        self.run(tmp_path, f"{self.RESTART_EXIT_CODE} 0")

        first, second = self.lines(tmp_path / "runs")
        assert first == second
        assert "--group cpu" in first

    @pytest.mark.parametrize("status", [0, 1, 143])
    def test_any_other_status_passes_straight_through(
        self, tmp_path: Path, status: int
    ):
        proc = self.run(tmp_path, f"{status} 0")

        assert proc.returncode == status
        assert len(self.lines(tmp_path / "runs")) == 1

    def test_a_pilot_job_event_runs_the_worker_once(self, tmp_path: Path):
        proc = self.run(tmp_path, "", "--pilot-job-event", "start")

        assert proc.returncode == 0
        (event,) = self.lines(tmp_path / "events")
        assert event.endswith("--pilot-job-event start")
        assert self.lines(tmp_path / "runs") == []

    def test_the_setup_script_runs_once_across_a_restart(self, tmp_path: Path):
        """A setup script need not be safe to run twice."""
        self.run(tmp_path, f"{self.RESTART_EXIT_CODE} 0")

        assert len(self.lines(tmp_path / "runs")) == 2
        assert self.lines(tmp_path / "setup") == ["setup"]

    def test_a_batch_worker_restarts_too(self, tmp_path: Path):
        """The batch shell sources the worker script,
        and the restart loop runs there.
        """
        script = render_template(
            "slurm_pilot:worker_sbatch_script",
            name="testex.job.cpu.0",
            work_dir="/scratch/work",
            is_batch_worker=True,
            worker_script_path=self.write_worker_script(tmp_path),
        )

        run_sbatch_script(
            script,
            tmp_path,
            SLURM_NTASKS="1",
            **self.env(tmp_path, f"{self.RESTART_EXIT_CODE} 0"),
        )

        assert len(self.lines(tmp_path / "runs")) == 2
        events = [
            ln.split("--pilot-job-event ")[-1] for ln in self.lines(tmp_path / "events")
        ]
        # The restart is not a new pilot job,
        # so the batch script reports each event once.
        assert events == ["start", "exit"]
        # Each event report runs the whole worker script in a bash of its own,
        # which runs the setup script too.
        # The worker adds one more run, and its restart adds none.
        assert len(self.lines(tmp_path / "setup")) == 3


class TestSbatchScriptTemplate:
    def test_renders_directives_in_order(self):
        out = render_template(
            "slurm_utils:script_template",
            name="myjob",
            sbatch_args=["-A alloc", "-p gpu", "--gres=gpu:1"],
            output_file="/work/myjob-%j.out",
            script="echo hi",
        )

        # The name first, then the caller's arguments in the caller's order,
        # then the output file.
        directives = [ln for ln in out.splitlines() if ln.startswith("#SBATCH")]
        assert directives == [
            '#SBATCH --job-name "myjob"',
            "#SBATCH -A alloc",
            "#SBATCH -p gpu",
            "#SBATCH --gres=gpu:1",
            '#SBATCH --output "/work/myjob-%j.out"',
        ]
        # `sbatch` reads directives only up to the first command,
        # so all of them come before the script body.
        lines = out.splitlines()
        assert lines[0] == "#!/bin/bash"
        assert lines.index("echo hi") > lines.index(directives[-1])
        assert out.rstrip().endswith("echo hi")

    def test_no_sbatch_args(self):
        out = render_template(
            "slurm_utils:script_template",
            name="myjob",
            sbatch_args=[],
            output_file="/work/out",
            script="true",
        )

        assert out.count("#SBATCH") == 2  # job-name and output only


class TestShellQuoting:
    """A value reaches the worker as one argument, whatever characters it holds."""

    AWKWARD = [
        "plain",
        "it's",
        "two words",
        "$HOME and `date`",
        'a "double" quote',
        "''",
        "",
    ]

    @pytest.mark.parametrize("value", AWKWARD)
    def test_bash_reads_back_the_value_it_was_given(self, value: str):
        result = subprocess.run(
            ["bash", "-c", "printf '%s' " + shell_quote(value)],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout == value

    def test_a_plain_value_renders_as_it_always_did(self):
        assert shell_quote("cpu") == "'cpu'"

    def test_the_worker_gets_each_value_whole(self, tmp_path: Path):
        stub = tmp_path / "stub-worker"
        # One argument per line, so a split or a lost quote shows.
        stub.write_text('#!/bin/bash\nprintf \'%s\\n\' "$@" > "$ARGS"\n')
        stub.chmod(0o755)
        work_dir = str(tmp_path / "it's a $dir")
        paths = json.dumps(["/home/o'brien/lib"])
        script = render_template(
            "slurm_pilot:worker_script",
            worker_exe=str(stub),
            setup_script="",
            group="cpu",
            name="testex.job.cpu.0",
            actor_class_name="pkg.Model's",
            server_address="10.0.0.1:5051",
            work_dir=work_dir,
            python_paths_json=paths,
            restart_exit_code=75,
        )
        script_path = tmp_path / "worker.sh"
        script_path.write_text(script)
        args_file = tmp_path / "args"

        subprocess.run(
            ["bash", str(script_path)],
            env={"PATH": os.environ["PATH"], "ARGS": str(args_file)},
            check=True,
        )

        assert args_file.read_text().splitlines() == [
            "--group",
            "cpu",
            "--name",
            "testex.job.cpu.0",
            "--actor-class-name",
            "pkg.Model's",
            "--server-address",
            "10.0.0.1:5051",
            "--work-dir",
            work_dir,
            "--python-paths-json",
            paths,
        ]
