import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


class NonSeekableWriter:
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        return self.stream.write(data)

    def flush(self):
        self.stream.flush()


def main():
    archive_dir = Path(sys.argv[1])
    # Hide stdout's seek/tell methods so zipfile writes a valid streaming archive.
    output = NonSeekableWriter(sys.stdout.buffer)
    with ZipFile(output, mode='w', compression=ZIP_DEFLATED) as zip_file:
        for file_path in archive_dir.rglob('*'):
            if file_path.is_file():
                archive_name = file_path.relative_to(archive_dir).as_posix()
                zip_file.write(file_path, arcname=archive_name)


if __name__ == '__main__':
    main()
