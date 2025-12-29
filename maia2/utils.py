import pyzstd


def decompress_zstd(compressed_file_path: str, decompressed_file_path: str) -> None:
    with open(compressed_file_path, "rb") as compressed_file, open(decompressed_file_path, "wb") as decompressed_file:
        # src -> dst: source file-like object, destination file-like object
        pyzstd.decompress_stream(compressed_file, decompressed_file)