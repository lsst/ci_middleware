lsst-ci-middleware v30.0.0 (2026-01-16)
=======================================

New Features
------------

- Use ``aggregate-graph`` to ingest QBB outputs. (`DM-52360 <https://jira.lsstcorp.org/browse/DM-52360>`_)


lsst-ci-middleware v28.0.0 (2024-11-20)
=======================================

New Features
------------

- Added tests for counts of expected quanta in ``pipetask report``. (`DM-44368 <https://jira.lsstcorp.org/browse/DM-44368>`_)
- Added tests for the ``QuantumProvenanceGraph``. (`DM-41711 <https://jira.lsstcorp.org/browse/DM-41711>`_)

lsst-ci-middleware v26.0.2 (2024-03-20)
=======================================

New Features
------------

- Added tests which exercise the human-readable option for ``pipetask report``. (`DM-41606 <https://jira.lsstcorp.org/browse/DM-41606>`_)

lsst-ci-middleware v26.0.1 (2024-01-30)
=======================================

New Features
------------

- Added tests for ``QuantumGraphExecutionReports`` using "rescues" to test reporting on fail-and-recover attempts for step 1. (`DM-37163 <https://jira.lsstcorp.org/browse/DM-37163>`_)

Other Changes and Additions
---------------------------

- Dropped support for Pydantic 1.x. (`DM-42302 <https://jira.lsstcorp.org/browse/DM-42302>`_)

lsst-ci-middleware v26.0.2 (2023-09-22)
=======================================

New Features
------------

- Added tests for ``--skip-existing-in`` and the "rescue" pattern. (`DM-39672 <https://jira.lsstcorp.org/browse/DM-39672>`_)
