"""Aggregate report for the attachment-text sub-stage.

Run after a real extraction to spot-check distribution and sample text:

    uv run python apps/pipeline/scripts/verify_attachment_text.py

Reads ``DATABASE_URL`` from the environment via ``rag_shared.get_pool``.
No writes — purely read-only queries against ``silver.attachment_text``.
"""

from __future__ import annotations

from rag_shared import get_pool


def _print_table(rows: list[tuple], headers: list[str]) -> None:
    if not rows:
        print("  (no rows)")
        return
    widths = [
        max(len(str(h)), max(len(str(r[i])) for r in rows))
        for i, h in enumerate(headers)
    ]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*("-" * w for w in widths)))
    for r in rows:
        print(fmt.format(*(str(c) for c in r)))


def main() -> None:
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        print("=" * 72)
        print("silver.attachment_text — verification report")
        print("=" * 72)

        cur.execute("select count(*) from silver.attachment_text")
        total = cur.fetchone()[0]
        print(f"\nTotal rows: {total}\n")

        print("Distribution by (extractor, status):")
        cur.execute(
            """
            select extractor, status, count(*) as n,
                   coalesce(sum(char_count), 0) as total_chars
            from silver.attachment_text
            group by 1, 2
            order by 1, 2
            """
        )
        _print_table(
            cur.fetchall(),
            headers=["extractor", "status", "count", "total_chars"],
        )

        print("\nDistinct extractor_version strings:")
        cur.execute(
            "select extractor_version, count(*) "
            "from silver.attachment_text group by 1 order by 1"
        )
        _print_table(cur.fetchall(), headers=["extractor_version", "count"])

        print("\nneeds_ocr storage paths (full list):")
        cur.execute(
            "select attachment_storage_path from silver.attachment_text "
            "where status='needs_ocr' order by attachment_storage_path"
        )
        for (sp,) in cur.fetchall():
            print(f"  {sp}")

        print("\nerror rows (full list):")
        cur.execute(
            "select attachment_storage_path, extractor, error_message "
            "from silver.attachment_text where status='error' "
            "order by attachment_storage_path"
        )
        rows = cur.fetchall()
        if not rows:
            print("  (none)")
        else:
            for sp, ex, msg in rows:
                print(f"  [{ex}] {sp}: {msg}")

        print("\nSample 'ok' text snippets (3 random):")
        cur.execute(
            """
            select attachment_storage_path,
                   substring(text, 1, 200) as snippet
            from silver.attachment_text
            where status='ok' and char_count > 0
            order by random()
            limit 3
            """
        )
        for sp, snippet in cur.fetchall():
            print(f"\n  --- {sp}")
            for line in (snippet or "").splitlines()[:6]:
                print(f"     {line}")

        print("\nDownstream readiness (rows usable by gold):")
        cur.execute(
            "select count(*) from silver.attachment_text "
            "where status='ok' and char_count > 0"
        )
        usable = cur.fetchone()[0]
        print(f"  status='ok' and char_count > 0: {usable}")

        print()


if __name__ == "__main__":
    main()
