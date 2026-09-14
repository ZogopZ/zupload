from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shlex
import shutil
import sys


@dataclass
class RunLogger:
    run_dir: Path
    spreadsheet: Path | None
    command_line: str
    before_ref: str

    @classmethod
    def start(
        cls,
        spreadsheet: str | Path | None = None,
        prefix: str = 'zupload',
    ) -> "RunLogger":
        logs_root = Path.cwd() / 'logs'
        logs_root.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d-%H%M%S')
        run_dir = logs_root / f'{prefix}-{ts}'
        suffix = 1
        while run_dir.exists():
            run_dir = logs_root / f'{prefix}-{ts}-{suffix}'
            suffix += 1
        run_dir.mkdir(parents=True, exist_ok=False)
        command_line = ' '.join(shlex.quote(arg) for arg in sys.argv)
        logger = cls(
            run_dir=run_dir,
            spreadsheet=None if spreadsheet is None else Path(spreadsheet),
            command_line=command_line,
            before_ref='',
        )
        logger.before_ref = logger._snapshot(which='before')
        return logger

    def finish(self, status: str = 'ok', error: str | None = None) -> None:
        after_ref = self._snapshot(which='after')
        lines = [
            f'command={self.command_line}',
            f'status={status}',
            f'spreadsheet_before={self.before_ref}',
            f'spreadsheet_after={after_ref}',
        ]
        if error:
            lines.append(f'error={error}')
        (self.run_dir / 'run.txt').write_text('\n'.join(lines) + '\n')

    def _snapshot(self, which: str) -> str:
        if self.spreadsheet is None:
            return 'spreadsheet path not set'
        if self.spreadsheet.exists():
            ext = self.spreadsheet.suffix or '.bin'
            target = self.run_dir / f'spreadsheet.{which}{ext}'
            shutil.copy2(self.spreadsheet, target)
            return str(self.spreadsheet.resolve())
        else:
            return f'not found: {self.spreadsheet}'
