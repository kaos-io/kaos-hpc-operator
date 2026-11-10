"""Holds the engine (the `kubecore/` package, mapped here when the wheel is built) and its data files.

Import nothing from here: the runner puts THIS directory on sys.path so that `kubecore` resolves to the
engine's copy. Installing `kubecore` at top level is forbidden (kaos-lib 0.4 ships a `kubecore` alias).
"""
