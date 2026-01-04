from dataclasses import dataclass


@dataclass
class ManifestEntry:
    """Represents a single entry in a manifest with statistics."""

    file_path: str
    file_format: str
    record_count: int
    file_size_in_bytes: int
    uncompressed_size_in_bytes: int
    min_k_hashes: list[list[int]]
    histogram_counts: list[list[int]]
    histogram_bins: int
    min_values: list
    max_values: list

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "file_format": self.file_format,
            "record_count": self.record_count,
            "file_size_in_bytes": self.file_size_in_bytes,
            "uncompressed_size_in_bytes": self.uncompressed_size_in_bytes,
            "min_k_hashes": self.min_k_hashes,
            "histogram_counts": self.histogram_counts,
            "histogram_bins": self.histogram_bins,
            "min_values": self.min_values,
            "max_values": self.max_values,
        }
