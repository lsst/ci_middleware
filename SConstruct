# -*- python -*-
from lsst.sconsUtils import scripts, state

# Python-only package
scripts.BasicSConstruct("ci_middleware", disableCc=True, noCfgFile=True)

state.env.CleanTree([".coverage*"])

state.env.Depends(state.targets["tests"], state.targets["data"])
