# This file is part of ci_middleware.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "PipelineCommands",
    "python_cmd",
    "tar_repo_cmd",
    "untar_repo_cmd",
)

import os
from collections.abc import Iterable, Sequence

from SCons.Script import File

from lsst.sconsUtils import state
from lsst.sconsUtils.utils import libraryLoaderEnvironment

from ._constants import DEFAULTS_COLLECTION, UNMOCKED_DATASET_TYPES

PIPETASK_BIN = os.path.join(os.environ["CTRL_MPEXEC_DIR"], "bin/pipetask")
BUTLER_BIN = os.path.join(os.environ["DAF_BUTLER_DIR"], "bin/butler")
COVERAGE_PACKAGES = ",".join(["lsst.daf.butler", "lsst.pipe.base", "lsst.ctrl.mpexec"])


def python_cmd(*args: str, expect_failure: bool = False) -> str:
    """Return a command-line string that runs the Python executable.

    Parameters
    ----------
    *args
        Command-line arguments to pass to Python.
    expect_failure : `bool`
        If `True`, expect the pipetask command to fail with a nonzero exit
        code, and guard it accordingly to keep the SCons build running.

    Returns
    -------
    cmd : `str`
        A command-line string.
    """
    terms = [libraryLoaderEnvironment(), "python"]
    terms.extend(args)
    if expect_failure:
        terms.extend(["||", "true"])
    return " ".join(terms)


def tar_repo_cmd(input_dir: str, output_tar: str) -> str:
    """Return a command-line string that tars up a data repository.

    Parameters
    ----------
    input_dir : `str`
        Path to the data repository.  Will be deleted as the tar archive is
        created.
    output_tar : `str`
        Name of the output tar file.  Should reflect gzip compression.

    Returns
    -------
    cmd : `str`
        A command-line string.

    Notes
    -----
    This adds all data repository files and directories directly to the root of
    the tar archive, so when extracted it will turn the current directory (not
    some subdirectory of it) into a data repository.  This makes it easier to
    extract the data repository into a location that is different from the
    original one.
    """
    return f"tar -czf {output_tar} -C {input_dir} . && rm -rf {input_dir}"


def untar_repo_cmd(source_tar: str, output_dir: str) -> str:
    """Return a command-line string that untars a data repository.

    Parameters
    ----------
    source_tar : `str`
        Path to the input tar file.  Should be gzip-compressed and hold data
        repository contents in the root of the archive, not some subdirectory
        (see `tar_repo_cmd`).
    output_dir : `str`
        Path to the output repository.

    Returns
    -------
    cmd : `str`
        A command-line string.
    """
    return f"rm -rf {output_dir} && mkdir {output_dir} && tar -C {output_dir} -xzf {source_tar}"


class PipelineCommands:
    """Helper class for using SCons to run pipelines.

    Parameters
    ----------
    name : `str`
        Identifier for this pipeline, to be used in file and collection names.
    pipeline_path : `str`
        Local filesystem path to the pipeline.
    base_repo : `SCons.Script.File`
        SCons file node for the tarred-up base input data repository.  This
        generally has most overall-input datasets for the pipeline, but any
        missing overall inputs will be added to a copy of this repository.
    chain_template : `str`, optional
        Format string for the CHAINED output collection; should have a ``name``
        placeholder.
    run_template : `str`, optional
        Format string for RUN output collections; should have ``name`` and
        ``suffix`` placeholders.

    Notes
    -----
    This class is intended to be used with a method chaining pattern: ```
    PipelineCommands(...).add(...).add(...).finish() ``` with one `add` call
    for each ``QuantumGraph``.  Each call will create build targets for:

    - the ``QuantumGraph`` file;
    - a ``tar``-archived data repository with the outputs from running the
      graph directly against the (SQLite) data repository using ``pipetask
      run``;
    - a ``tar``-archived data repository with the outputs from running the
      graph with `lsst.daf.butler.QuantumBackedButler`.  A copy of this data
      repository is used as the input data repository for the next `add` step.

    This approach of using ``tar`` archives to copy data repositories for each
    step does lead to a lot of I/O, but it also has some major advantages:

    - SCons is much better at managing dependencies between file targets than
      directory targets.

    - Archiving the results of each step as a tar file makes it very easy to
      inspect the outputs in detail, or debug problems with them, without
      worrying about "pollution" from other steps.

    Constructing the `PipelineCommands` instance also creates a
    ``tar``-archived data repository with all overall-inputs to the pipeline
    and an output CHAINED collection initialized to the input collections.
    """

    def __init__(
        self,
        name: str,
        pipeline_path: str,
        base_repo: File,
        chain_template: str = "HSC/runs/{name}",
        run_template: str = "HSC/runs/{name}/{suffix}",
    ):
        self.name = name
        self.pipeline_path = pipeline_path
        self.chain = chain_template.format(name=self.name)
        self.run_template = run_template
        self.all_targets: list[File] = []
        inputs_repo = self._make_inputs_repo(base_repo)
        self.last_direct_repo: File = inputs_repo
        self.last_qbb_repo: File = inputs_repo

    def add(
        self,
        step: str | None = None,
        group: str | None = None,
        where: str = "",
        fail: Sequence[str] = (),
        skip_existing_in_last: bool = False,
        auto_retry_mem: tuple[str, str] | None = None,
    ) -> PipelineCommands:
        """Add a new QuantumGraph and its execution to the build.

        Parameters
        ----------
        step : `str`, optional
            Named subset corresponding to a step in the pipeline.  If `None`
            (default), a graph is generated for the full pipeline.
        group : `str`, optional
            Additional identifier to include in the collection name and output
            files that reflects how the ``where`` subdivides the data or the
            attempt being made.  This is used when a single pipeline step is
            being split up into multiple graphs, to simulation production runs
            at scales where this is necessary, or when creating "rescue" graphs
            to finish up processing that partially failed.
        where : `str`, optional
            Data ID constraint expression passed as the ``--data-query``
            argument to ``pipetask`` when building the graph.
        fail : `~collections.abc.Sequence` [ `str` ]
            Sequence of colon-separated ``task_label:error_type:where`` tuples
            that identify quanta that should raise an exception.
        skip_existing_in_last : `bool`, optional
            If `True`, pass ``--skip-existing-in`` to the QuantumGraph
            generation command with the input collections as the argument.
        auto_retry_mem : `tuple` [ `str`, `str` ], optional
            If not `None`, run all quanta twice, with ``--memory-per-quantum``
            passed with the given values.  This can be combined with a
            ``fail`` setting that causes the first attempt for one or more
            quanta to simulate an out-of-memory failure for the first value
            only, effectively testing BPS auto-retry logic.

        Returns
        -------
        self : `PipelineCommands`
            The instance this method was called on, to facilitate
            method-chaining.
        """
        if step is None:
            suffix = "full"
        else:
            suffix = step
        if group is not None:
            suffix = f"{suffix}-{group}"
        output_run = self.run_template.format(name=self.name, suffix=suffix)
        qg = self._add_qg(
            suffix=suffix,
            output_run=output_run,
            tasks_or_subsets=[step] if step is not None else [],
            where=where,
            fail=fail,
            skip_existing_in_last=skip_existing_in_last,
        )
        self.last_direct_repo = self._add_direct(
            qg,
            suffix=suffix,
            output_run=output_run,
            expect_failure=bool(fail),
            auto_retry_mem=auto_retry_mem,
        )
        self.last_qbb_repo = self._add_qbb(
            qg,
            suffix=suffix,
            output_run=output_run,
            expect_failure=bool(fail),
            auto_retry_mem=auto_retry_mem,
        )
        self.last_output_run = output_run
        return self

    def add_side_run(
        self,
        suffix: str,
        tasks_or_subsets: Iterable[str],
        where: str = "",
        fail: Sequence[str] = (),
        skip_existing_in_last: bool = False,
        extend_run: bool = False,
        clobber_outputs: bool = False,
        raise_on_partial_outputs: bool = False,
        expect_failure: bool | None = None,
    ) -> PipelineCommands:
        """Add a new QuantumGraph and its execution to the build, but do not
        use it as input for later runs.

        Parameters
        ----------
        suffix : `str`
            Suffix for filenames and run collections that uniquely identifies
            this run.
        tasks_or_subsets : `~collections.abc.Iterable` [ `str` ]
            Named subset or task labels to run.  If empty, the full pipeline is
            run.
        where : `str`, optional
            Data ID constraint expression passed as the ``--data-query``
            argument to ``pipetask`` when building the graph.
        fail : `~collections.abc.Sequence` [ `str` ]
            Sequence of colon-separated ``task_label:error_type:where`` tuples
            that identify quanta that should raise an exception.
        skip_existing_in_last : `bool`, optional
            If `True`, pass ``--skip-existing-in`` to the QuantumGraph
            generation command with the input collections as the argument.
        extend_run : `bool`, optional
            If `True`, pass ``--extend-run`` to the QuantumGraph generation
            command and the direct execution command, and do not run the
            pre-exec-init logic for QBB execution.
        clobber_outputs : `bool`, optional
            If `True`, pass ``--clobber-outputs`` to the QuantumGraph
            generation command and the direct execution command.  QBB execution
            effectively inherits its clobber behavior from the quantum graph.
        raise_on_partial_outputs : `bool`, optional
            If `True`, pass ``--raise-on-partial-outputs`` to ``pipetask run``
            and ``pipetask run-qbb``.
        expect_failure : `bool`, optional
            Override whether to expect the command to exit with a nonzero exit
            code; default is ``bool(fail)``.

        Returns
        -------
        self : `PipelineCommands`
            The instance this method was called on, to facilitate
            method-chaining.
        """
        if extend_run:
            output_run = self.last_output_run
        else:
            output_run = self.run_template.format(name=self.name, suffix=suffix)
        qg = self._add_qg(
            suffix=suffix,
            output_run=output_run,
            tasks_or_subsets=tasks_or_subsets,
            where=where,
            fail=fail,
            skip_existing_in_last=skip_existing_in_last,
            extend_run=extend_run,
            clobber_outputs=clobber_outputs,
        )
        self._add_direct(
            qg,
            suffix=suffix,
            output_run=output_run,
            expect_failure=bool(fail) if expect_failure is None else expect_failure,
            extend_run=extend_run,
            clobber_outputs=clobber_outputs,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )
        if not extend_run:
            self._add_qbb(
                qg,
                suffix=suffix,
                output_run=output_run,
                expect_failure=bool(fail) if expect_failure is None else expect_failure,
                pre_exec_init=not extend_run,
                raise_on_partial_outputs=raise_on_partial_outputs,
            )
        return self

    def finish(self) -> list[File]:
        """Finish adding the SCons targets for this pipeline.

        This method must be called after all `add` calls, and `add` may not
        be called after it has been called.  It is considered an implementation
        detail whether SCons targets are created by this call or in `add`
        itself.

        Returns
        -------
        nodes : `list` [ `SCons.Script.File` ]
            List of SCons file-target nodes, representing everything produced
            for this pipeline.
        """
        self.all_targets.extend(
            state.env.Command(
                [File(f"{self.name}/direct.tgz")],
                [self.last_direct_repo],
                ["ln -s ${SOURCE.abspath} ${TARGET}"],
            )
        )
        self.all_targets.extend(
            state.env.Command(
                [File(f"{self.name}/qbb.tgz")],
                [self.last_qbb_repo],
                ["ln -s ${SOURCE.abspath} ${TARGET}"],
            )
        )
        return self.all_targets

    def _make_inputs_repo(self, base_repo: File) -> File:
        """Make a SCons target for the pipeline-input data repository.

        Parameters
        ----------
        base_repo : `SCons.Script.File`
            SCons file node for the tarred-up base input data repository.  This
            generally has most overall-input datasets for the pipeline, but any
            missing overall inputs will be added to a copy of this repository.

        Returns
        -------
        inputs_repo : `SCons.Script.File`
            SCons file node for the input repo.
        """
        repo_file = f"{self.name}/inputs.tgz"
        repo_in_cmd = "${TARGET.base}"
        targets = state.env.Command(
            [repo_file],  # target
            [base_repo, File(self.pipeline_path)],  # sources
            [
                untar_repo_cmd("${SOURCE}", repo_in_cmd),
                python_cmd(
                    "-m lsst.ci.middleware prep-for-pipeline",
                    repo_in_cmd,  # data repository
                    "${SOURCES[1]}",  # pipeline file path
                ),
                # Add the output collection up front as a flattened version
                # of the inputs.  Execution steps will prepend to this.
                python_cmd(
                    BUTLER_BIN,
                    "collection-chain",
                    repo_in_cmd,
                    "--flatten",
                    self.chain,
                    DEFAULTS_COLLECTION,
                ),
                tar_repo_cmd(repo_in_cmd, "${TARGET}"),
            ],
        )
        self.all_targets.extend(targets)
        return targets[0]

    def _add_qg(
        self,
        suffix: str,
        output_run: str,
        tasks_or_subsets: Iterable[str],
        where: str,
        fail: Sequence[str] = (),
        skip_existing_in_last: bool = False,
        extend_run: bool = False,
        clobber_outputs: bool = False,
    ) -> File:
        """Make a SCons target for the quantum graph file.

        Parameters
        ----------
        suffix : `str`
            Suffix that combines the step and group, if present.
        output_run : `str`
            Name of the output RUN collection.
        tasks_or_subsets : `~collections.abc.Iterable [ `str` ]
            Named subset or task labels to run.  If empty, the full pipeline is
            run.
        where : `str`, optional
            Data ID constraint expression passed as the ``--data-query``
            argument to ``pipetask`` when building the graph.
        fail : `~collections.abc.Sequence` [ `str` ]
            Sequence of colon-separated ``task_label:error_type:where`` tuples
            that identify quanta that should raise an exception.
        skip_existing_in_last : `bool`, optional
            If `True`, pass ``--skip-existing-in`` to the QuantumGraph
            generation command with the input collections as the argument.
        extend_run : `bool`, optional
            If `True`, pass ``--extend-run`` to the QuantumGraph generation
            command.
        clobber_outputs : `bool`, optional
            If `True`, pass ``--clobber-outputs`` to the QuantumGraph
            generation command.

        Returns
        -------
        qg_file : `SCons.Script.File`
            SCons file node for the quantum graph.
        """
        qg_file = os.path.join(self.name, suffix + ".qgraph")
        log = os.path.join(self.name, suffix + "-qgraph.log")
        repo_in_cmd = "${TARGETS[0].base}-qgraph-repo"
        fail_and_retry_args = [f"--mock-failure {f}" for f in fail]
        if skip_existing_in_last:
            fail_and_retry_args.append(f"--skip-existing-in {self.chain}")
        if extend_run:
            fail_and_retry_args.append("--extend-run")
        if clobber_outputs:
            fail_and_retry_args.append("--clobber-outputs")
        targets = state.env.Command(
            [File(qg_file), File(log)],
            # We always build QGs and run direct processing using the previous
            # QBB repo - we expect QBB execution to be _slightly_ faster (no
            # SQLite locking or other full-butler overheads), and this allows
            # the direct executions for different steps/groups to be executed
            # in parallel if the serial QBB branch gets out in front.
            [self.last_qbb_repo, File(self.pipeline_path)],
            [
                # Untar the input data repository, which naturally makes a copy
                # of it, with the name we'll use for the output data
                # repository.  This will be a temporary repo that we'll throw
                # away after the QG is built.
                untar_repo_cmd("${SOURCES[0]}", repo_in_cmd),
                # Build the QG, telling it to save the logs as well.
                self._pipetask_cmd(
                    "qgraph",
                    f"-b {repo_in_cmd}",
                    (
                        "-p ${SOURCES[1]}"
                        + ("" if tasks_or_subsets is None else f"#{','.join(tasks_or_subsets)}")
                    ),
                    f'-d "{where}"',
                    f"--input {DEFAULTS_COLLECTION}",
                    f"--output {self.chain}",
                    f"--output-run {output_run}",
                    "--save-qgraph ${TARGETS[0]}",
                    "--qgraph-datastore-records",
                    "--mock",
                    *fail_and_retry_args,
                    f"--unmocked-dataset-types '{','.join(UNMOCKED_DATASET_TYPES)}'",
                    log="${TARGETS[1]}",
                ),
                f"rm -r {repo_in_cmd}",
            ],
        )
        self.all_targets.extend(targets)
        return targets[0]

    def _add_direct(
        self,
        qg_file: File,
        suffix: str,
        output_run: str,
        expect_failure: bool = False,
        extend_run: bool = False,
        clobber_outputs: bool = False,
        auto_retry_mem: tuple[str, str] | None = None,
        raise_on_partial_outputs: bool = False,
    ) -> File:
        """Make an SCons target for direct execution of the quantum graph
        with ``pipetask run`` and a full butler.

        Parameters
        ----------
        qg_file : `SCons.Script.File`
            SCons file node for the quantum graph.
        suffix : `str`
            Suffix that combines the step and group, if present.
        output_run : `str`
            Name of the output RUN collection.
        expect_failure : `bool`, optional
            If `True`, expect the pipetask command to fail with a nonzero exit
            code, and guard it accordingly to keep the SCons build running.
        extend_run : `bool`, optional
            If `True`, pass ``--extend-run`` to ``pipetask run``.
        clobber_outputs : `bool`, optional
            If `True`, pass ``--clobber-outputs`` to ``pipetask run``.
        auto_retry_mem : `tuple` [ `str`, `str` ], optional
            See argument of the same name on `add`.
        raise_on_partial_outputs : `bool`, optional
            If `True`, pass ``--raise-on-partial-outputs`` to ``pipetask run``.

        Returns
        -------
        inputs_repo : `SCons.Script.File`
            SCons file node for the output repo.
        """
        repo_file = os.path.join(self.name, suffix + "-direct.tgz")
        log = os.path.join(self.name, suffix + "-direct.log")
        repo_in_cmd = "${TARGETS[0].base}"
        extra_args = []
        if extend_run:
            extra_args.append("--extend-run")
        if clobber_outputs:
            extra_args.append("--clobber-outputs")
        if auto_retry_mem:
            extra_args.append(f"--memory-per-quantum {auto_retry_mem[0]}")
        if raise_on_partial_outputs:
            extra_args.append("--raise-on-partial-outputs")
        else:
            extra_args.append("--no-raise-on-partial-outputs")
        cmds = [
            # Untar the input data repository, which naturally makes a copy
            # of it, with the name we'll use for the output data
            # repository.
            untar_repo_cmd("${SOURCES[0]}", repo_in_cmd),
            # Execute the QG using the full original butler.
            self._pipetask_cmd(
                "run",
                f"-b {repo_in_cmd}",
                "-g ${SOURCES[1]}",
                f"--input {DEFAULTS_COLLECTION}",
                f"--output {self.chain}",
                f"--output-run {output_run}",
                "--register-dataset-types",
                *extra_args,
                log="${TARGETS[1]}",
                expect_failure=expect_failure,
            ),
        ]
        if auto_retry_mem:
            cmds.append(
                self._pipetask_cmd(
                    "run",
                    f"-b {repo_in_cmd}",
                    "-g ${SOURCES[1]}",
                    f"--input {DEFAULTS_COLLECTION}",
                    f"--output {self.chain}",
                    f"--output-run {output_run}",
                    "--register-dataset-types",
                    "--extend-run",
                    "--clobber-outputs",
                    f"--memory-per-quantum {auto_retry_mem[1]}",
                    log="${TARGETS[1]}",
                    expect_failure=False,
                )
            )
        cmds.append(tar_repo_cmd(repo_in_cmd, "${TARGETS[0]}"))
        targets = state.env.Command(
            [File(repo_file), File(log)],
            # We use the last QBB repo as input, even for direct executions
            # (see comments in _add_qg for why).
            [self.last_qbb_repo, qg_file],
            cmds,
        )
        self.all_targets.extend(targets)
        return targets[0]

    def _add_qbb(
        self,
        qg_file: File,
        suffix: str,
        output_run: str,
        expect_failure: bool,
        pre_exec_init: bool = True,
        auto_retry_mem: tuple[str, str] | None = None,
        raise_on_partial_outputs: bool = False,
    ) -> File:
        """Make an SCons target for direct execution of the quantum graph
        with ``pipetask run-qbb`` and `lsst.daf.butler.QuantumBackedButler`.

        Parameters
        ----------
        qg_file : `SCons.Script.File`
            SCons file node for the quantum graph.
        suffix : `str`
            Suffix that combines the step and group, if present.
        output_run : `str`
            Name of the output RUN collection.
        expect_failure : `bool`
            If `True`, expect the pipetask command to fail with a nonzero exit
            code, and guard it accordingly to keep the SCons build running.
        pre_exec_init : `bool`, optional
            If `False`, do not run ``pipetask pre-exec-init-qbb`` at all.
        auto_retry_mem : `tuple` [ `str`, `str` ], optional
            See argument of the same name on `add`.
        raise_on_partial_outputs : `bool`, optional
            If `True`, pass ``--raise-on-partial-outputs`` to
            ``pipetask run-qbb``.

        Returns
        -------
        inputs_repo : `SCons.Script.File`
            SCons file node for the output repo.
        """
        repo_file = os.path.join(self.name, suffix + "-qbb.tgz")
        log = os.path.join(self.name, suffix + "-qbb.log")
        repo_in_cmd = "${TARGETS[0].base}"
        commands = [
            # Untar the input data repository, which naturally makes a copy of
            # it, with the name we'll use for the output data repository.
            untar_repo_cmd("${SOURCES[0]}", repo_in_cmd)
        ]
        if pre_exec_init:
            commands.append(
                # Run pre-execution steps via QuantumBackedButler.
                self._pipetask_cmd(
                    "pre-exec-init-qbb",
                    repo_in_cmd,
                    "${SOURCES[1]}",
                    log="${TARGETS[1]}",
                )
            )
        extra_args: list[str] = []
        if auto_retry_mem:
            extra_args.append(f"--memory-per-quantum {auto_retry_mem[0]}")
        if raise_on_partial_outputs:
            extra_args.append("--raise-on-partial-outputs")
        else:
            extra_args.append("--no-raise-on-partial-outputs")
        commands.append(
            # Execute the QG using QuantumBackedButler.
            self._pipetask_cmd(
                "run-qbb",
                repo_in_cmd,
                "${SOURCES[1]}",
                *extra_args,
                log="${TARGETS[1]}",
                expect_failure=expect_failure,
            )
        )
        if auto_retry_mem:
            commands.append(
                self._pipetask_cmd(
                    "run-qbb",
                    repo_in_cmd,
                    "${SOURCES[1]}",
                    f"--memory-per-quantum {auto_retry_mem[1]}",
                    log="${TARGETS[1]}",
                    expect_failure=False,
                )
            )
        commands += [
            # Bring results home using butler transfer-from-graph.
            python_cmd(
                BUTLER_BIN,
                "transfer-from-graph",
                "${SOURCES[1]}",
                repo_in_cmd,
                "--no-transfer-dimensions",
                "--update-output-chain",
                "--register-dataset-types",
            ),
            tar_repo_cmd(repo_in_cmd, "${TARGETS[0]}"),
        ]
        targets = state.env.Command(
            [File(repo_file), File(log)],
            [self.last_qbb_repo, qg_file],
            commands,
        )
        self.all_targets.extend(targets)
        return targets[0]

    def _pipetask_cmd(self, subcommand: str, *args: str, log: str, expect_failure: bool = False) -> str:
        """Return a command-line string that runs ``pipetask``` with options
        common to all invocations for this pipeline.

        Parameters
        ----------
        subcommand : `str`
            Subcommand to run.
        *args
            Command-line arguments to pass to Python just after the subcommand.
        log : `str`
            Name of the file to write logs to.
        expect_failure : `bool`
            If `True`, expect the pipetask command to fail with a nonzero exit
            code, and guard it accordingly to keep the SCons build running.

        Returns
        -------
        cmd : `str`
            A command-line string.
        """
        return python_cmd(
            PIPETASK_BIN,
            "--long-log",
            f"--log-file {log}",
            "--no-log-tty",
            subcommand,
            *args,
            "--coverage",
            f"--cov-packages {COVERAGE_PACKAGES}",
            "--no-cov-report",
            expect_failure=expect_failure,
        )
