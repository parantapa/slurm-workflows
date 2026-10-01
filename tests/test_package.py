"""Tests for what the package exports."""

from __future__ import annotations

import pytest

import slurm_workflows


class TestExports:
    def test_every_exported_name_resolves(self):
        for name in slurm_workflows.__all__:
            assert getattr(slurm_workflows, name) is not None

    # One moved name stands for all of them,
    # since every one goes through the same branch of `__getattr__`.
    def test_a_moved_name_points_to_its_new_package(self):
        with pytest.raises(ImportError, match="slurm_workflows_optimize"):
            getattr(slurm_workflows, "IntRange")

    def test_a_from_import_of_a_moved_name_fails_the_same_way(self):
        with pytest.raises(ImportError, match="slurm-workflows-optimize"):
            from slurm_workflows import ExploreSpaceSobolQMC  # noqa: F401

    def test_an_unknown_name_is_an_attribute_error(self):
        with pytest.raises(AttributeError):
            getattr(slurm_workflows, "NoSuchName")
