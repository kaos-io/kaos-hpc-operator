"""KAOS HPC runner: the controller-owned MeluXina (Slurm) runner of PRD-HPC-1461 phase 1.

The proven in-pod waiter of hera-pipeline-template (`kubecore/meluxina.py`, dev be9e52b), split into
modules, without its admission client, reporting to its own HPCArgoWorkflowJob. Python 3, stdlib only.
"""
