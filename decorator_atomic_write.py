import os
import tempfile
import shutil
import stat
import pyarrow.parquet as pq


def atomic_operation(func):
    def wrapper(file_contents, target_file_path, mode):
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            dir=os.path.dirname(target_file_path))
        try:
            if os.path.exists(target_file_path):
                # copy metadata
                shutil.copy2(target_file_path, temp_file.name)
                st = os.stat(target_file_path)
                # copy owner and group
                os.chown(temp_file.name, st[stat.ST_UID], st[stat.ST_GID])
            func(file_contents=file_contents, file=temp_file.name, mode=mode)

            os.replace(temp_file.name, target_file_path)
        finally:
            if os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except:
                    pass

    return wrapper


@atomic_operation
def write_file(file_contents, file, mode):
    if mode == 'string_w':
        with open(file, 'w') as f:
            f.write(file_contents)
            f.flush()
            os.fsync(f.fileno())
    if mode == 'parquet_w':
        pq.write_table(file_contents, file)
