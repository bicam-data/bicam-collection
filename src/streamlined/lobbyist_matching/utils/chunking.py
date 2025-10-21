def create_progressive_chunks(
    text: str,
    reprocessing_attempts: int = 0,
    base_max_chunk_size: int = 500,
    min_chunk_size: int = 50,
) -> list[str]:
    """Create smaller chunks as attempts increase (500 -> 250 -> 125 -> ...)."""
    max_chunk_size = max(
        min_chunk_size, base_max_chunk_size // (2**reprocessing_attempts)
    )
    if not text:
        return []
    if len(text) <= max_chunk_size:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chunk_size, len(text))
        # Prefer sentence boundary or whitespace near boundary
        if end < len(text):
            search_range = min(50, max_chunk_size // 2)
            search_start = max(start + min_chunk_size, end - search_range)
            # Sentence boundary
            i = end - 1
            found = False
            while i >= search_start:
                if text[i] in ".!?":
                    end = i + 1
                    found = True
                    break
                i -= 1
            if not found:
                i = end - 1
                while i >= search_start:
                    if text[i].isspace():
                        end = i + 1
                        break
                    i -= 1
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end
    return chunks
