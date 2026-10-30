"""Source and runtime locations; runtime files stay outside tracked source."""
import os
from pathlib import Path

ASSETS = Path(__file__).resolve().parent
FINAL_SOURCE = ASSETS.parent
RUNTIME = Path(os.environ.get('CASCADE_RUNTIME_DIR', str(FINAL_SOURCE/'.runtime'))).expanduser().resolve()
FINAL2_SOURCE = Path(os.environ.get('CASCADE_FINAL2_DIR', str(FINAL_SOURCE/'.runtime/repos/Final-2'))).expanduser().resolve()
SOURCES = {'final': FINAL_SOURCE, 'final2': FINAL2_SOURCE}
