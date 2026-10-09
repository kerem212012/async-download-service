import asyncio
from io import BytesIO
import logging
import os
from pathlib import Path
import re
from zipfile import ZIP_DEFLATED, ZipFile

from aiohttp import web
import aiofiles


ARCHIVE_CHUNK_SIZE = 64 * 1024
ARCHIVE_CHUNK_DELAY = float(os.environ.get('ARCHIVE_CHUNK_DELAY', '0'))
if ARCHIVE_CHUNK_DELAY < 0:
    raise ValueError('ARCHIVE_CHUNK_DELAY must not be negative')
logger = logging.getLogger(__name__)


async def archive(request):
    archive_hash = request.match_info['archive_hash']
    if re.fullmatch(r'[A-Za-z0-9_-]+', archive_hash) is None:
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    project_dir = Path(__file__).resolve().parent
    photos_dir = project_dir / 'photos'
    if not photos_dir.is_dir():
        photos_dir = project_dir / 'test_photos'

    archive_dir = photos_dir / archive_hash
    if archive_dir.resolve().parent != photos_dir.resolve() or not archive_dir.is_dir():
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    def build_archive():
        archive_buffer = BytesIO()
        with ZipFile(archive_buffer, mode='w', compression=ZIP_DEFLATED) as zip_file:
            for file_path in archive_dir.rglob('*'):
                if file_path.is_file():
                    archive_name = file_path.relative_to(archive_dir).as_posix()
                    zip_file.write(file_path, arcname=archive_name)
        return archive_buffer.getvalue()

    loop = asyncio.get_event_loop()
    archive_contents = await loop.run_in_executor(None, build_archive)
    response = web.StreamResponse(
        headers={
            'Content-Type': 'application/zip',
            'Content-Disposition': (
                f'attachment; filename="photos-{archive_hash}.zip"'
            )
        },
    )
    await response.prepare(request)
    try:
        for offset in range(0, len(archive_contents), ARCHIVE_CHUNK_SIZE):
            if ARCHIVE_CHUNK_DELAY > 0:
                await asyncio.sleep(ARCHIVE_CHUNK_DELAY)
            chunk = archive_contents[offset:offset + ARCHIVE_CHUNK_SIZE]
            logger.debug('Sending archive chunk ... (%d bytes)', len(chunk))
            await response.write(chunk)
        await response.write_eof()
    except asyncio.CancelledError:
        logger.debug('Archive download cancelled for %s', archive_hash)
        raise
    except ConnectionError:
        logger.debug('Client disconnected while downloading archive %s', archive_hash)
    return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r', encoding='utf-8') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.DEBUG)
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}', archive),
        web.get('/archive/{archive_hash}/', archive),
    ])
    web.run_app(app)
