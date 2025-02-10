ci_middleware
=============

`ci_middleware` is an extension package for the [LSST Science Pipelines](https://pipelines.lsst.io/v/weekly>).

It provides integration tests for the [`daf_butler`](https://github.com/lsst/daf_butler.git), [`pipe_base`](https://github.com/lsst/pipe_base.git), and [`ctrl_mpexec`](https://github.com/lsst/ctrl_mpexec.git), as well as some unit tests for those packages that require realistically complex data and/or pipelines.

The test data used here was constructed from real observation metadata for 12 visits of the [Subaru Hyper Suprime-Cam Strategic Survey Program (HSC SSP)](https://hsc.mtk.nao.ac.jp/ssp/) in the COSMOS field, for the central 6 detectors only (IDs 41, 42, 49, 50, 57, and 58).
These span a range of filters and overlap relationships.
Instrument and observation metadata is stored in JSON files in the git repository, along with the configuration for a small skymap and definitions of a set of dataset types that are used as inputs by most pipelines.

No actual image or other data files are used by this package - instead, most datasets are tiny JSON files representing `lsst.pipe.base.tests.mocks.MockDataset` instances, which mimic some real dataset in terms of their dimensions and storage class relationships, while recording various I/O operations for easier testing.
The pipelines run are replaced by similarly mocked versions of themselves prior to QuantumGraph generation, allowing them to read and write the mocked datasets but share graph structure with the original pipeline.
See the [`pipe_base` mocks documentation](https://pipelines.lsst.io/v/weekly/modules/lsst.pipe.base/testing-pipelines-with-mocks.html) for details on how this works.

Package Structure and Overview
------------------------------

- `data`:
    This directory's `SConscript` defines `scons` build targets that set up input data repositories and run mocked versions of the HSC `ci_hsc`, `RC2` and `Prod` pipelines from the [`drp_pipe`](https://github.com/lsst/drp_pipe.git) package.
    Data repositories are packed into and unpacked from `tar` archives for each step, preserving the exact state of the repository as processing proceeds and making build target dependency management cleaner (`scons` is much better at handling atomic files than directories as targets).

- `doc`:
    Scripts for building package documentation with [`documenteer`](https://documenteer.lsst.io/guides/index.html).
    At present this only includes API reference docs extracted from Python code, since this package is not part of the `pipelines.lsst.io` build and hence it makes more sense to put all overview documentation in this README.

- `python/lsst/ci/middleware/data`:
    Configuration and JSON data files for setting up initial data repositories.
    These are in a subdirectory of the Python package to allow access through `resource://` URIs and [`ResourcePath`](https://github.com/lsst/resources.git).

- `python/lsst/ci/middleware/__main__.py`:
    Command-line interface to certain test-data management tools, intended to be run via `python -m lsst.ci.middleware`.
    This includes making new data repositories with the packaged dimension records, adding mocked input datasets for a particular pipeline, and displaying the spatial relationships between dimensions.
    Use `--help` for more information.

- `python/lsst/ci/middleware/_constants.py`:
    Global constants used by both the main Python package modules and SCons build scripts.
    This is the only module imported automatically by the package, because we do not want SCons build scripts to import anything they don't absolutely need.

- `python/lsst/ci/middleware/display.py`:
    A simple display tool for inspecting the spatial relationships between the visits, detectors, tracts, patches, and HTM7 trixels.

- `python/lsst/ci/middleware/mock_dataset_maker.py`:
    The `MockDatasetMaker` class, which can populate a data repository with the inputs needed to run a pipeline.

- `python/lsst/ci/middleware/output_repo_tests.py`:
    Helper code for `unittest`-based tests that check the contents of the data repository `tar` files in the `data` directory.

- `python/lsst/ci/middleware/repo_data.py`:
    Code for initializing up test data repositories and adjusting the `git`-managed test data itself.

- `python/lsst/ci/middleware/scons.py`:
    The `PipelineCommands` class, which is used by `data/SConscript` to set up pipeline execution build targets.

- `tests`:
    Unit test scripts that are run after the data repository `tar` files in `data` have been created.

Inspecting data repository `tar` files manually
-----------------------------------------------

The `data/SConscript` intentionally uses only command-line tools that can also be run by users, so re-executing the commands it reports should always work (provided the dependencies have been built, of course).

The most important thing to remember is that the data repository `tar` files built by this package hold the root of the data repository directly, so unpacking them will insert `butler.yaml`, `gen3.sqlite3`, etc into the current directory.
To unpack into a new directory, as is usually desired, use::

    mkdir new-directory && tar -C new-directory -xzf data/<pipeline>/<repo>.tgz

Test Coverage
-------------

When building QuantumGraphs and running them via `data/SConscript`, the `pipetask` tool is configured to write coverage information to `.coverage*` files in the root directory of the package.
These can be combined with coverage information from the unit tests for various middleware packages in order to generate a complete coverage report using [`coverage combine`](https://coverage.readthedocs.io/en/7.2.6/cmd.html#combining-data-files-coverage-combine).

Parallelization and Timing
--------------------------

The SCons build tree defined by `data/SConscript` looks more or less like three independent serial chains (alternating QuantumGraph generation and QuantumBackedButler execution for each of the three pipelines), with independent direct-execution steps branching off after each QuantumGraph generation step.
As a result, running SCons with more than `-j4` essentially does not help at all, with the minimum time to execute all test runs and unit test scripts around 20 minutes.

It might be possible to improve this by intercepting the number of cores passed such that SCons only sees a subset of these, and instead using the reserved cores to pass `-j2` (or larger) to `pipetask run`.
In addition to being tricky to implement, it's quite likely that this would quickly run into I/O as a new limit on parallelization anyway, since mocked pipeline execution involves reading and writing many small JSON and config files and not much else.
