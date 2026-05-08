"""Silver sub-stage: extract text from PDF/DOCX/CSV attachments (PRD #3).

See ``runner.run_attachment_text`` for the entry point. The silver dispatcher
in ``pipeline.silver`` calls it after the build stage by default.
"""

from .runner import run_attachment_text

__all__ = ["run_attachment_text"]
