import asyncio
import argparse
from contextlib import suppress
import logging
import os
from pathlib import Path
import re
import sys

from aiohttp import web
import aiofiles


ARCHIVE_CHUNK_SIZE = 64 * 1024
logger = logging.getLogger(__name__)


def parse_bool(value):
    normalized_value = value.lower()
    if normalized_value in ('1', 'true', 'yes', 'on'):
        return True
    if normalized_value in ('0', 'false', 'no', 'off'):
        return False
    raise argparse.ArgumentTypeError(
        'expected one of: true, false, yes, no, on, off, 1, 0'
    )


def parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Serve downloadable photo archives.')
    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument(
        '--log',
        dest='logging_enabled',
        action='store_true',
        help='enable debug logging',
    )
    logging_group.add_argument(
        '--no-log',
        dest='logging_enabled',
        action='store_false',
        help='disable logging',
    )
    parser.set_defaults(
        logging_enabled=parse_bool(os.getenv('LOGGING', 'true'))
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=float(os.getenv('ARCHIVE_CHUNK_DELAY', '0')),
        help='delay in seconds before sending each archive chunk',
    )
    parser.add_argument(
        '--photos-dir',
        type=Path,
        default=os.getenv('PHOTOS_DIR'),
        help='directory containing archive subdirectories',
    )
    options = parser.parse_args(args)
    if options.delay < 0:
        parser.error('--delay must not be negative')
    if options.photos_dir is not None:
        options.photos_dir = options.photos_dir.resolve()
    return options


def configure_logging(enabled):
    logging.disable(logging.NOTSET)
    if enabled:
        logging.basicConfig(level=logging.INFO)
        logger.setLevel(logging.DEBUG)
    else:
        logging.disable(logging.CRITICAL)


async def archive(request):
    archive_hash = request.match_info['archive_hash']
    if re.fullmatch(r'[A-Za-z0-9_-]+', archive_hash) is None:
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    project_dir = Path(__file__).resolve().parent
    photos_dir = request.app.get('photos_dir')
    if photos_dir is None:
        photos_dir = project_dir / 'photos'
        if not photos_dir.is_dir():
            photos_dir = project_dir / 'test_photos'
    else:
        photos_dir = Path(photos_dir)

    archive_dir = photos_dir / archive_hash
    if (
        archive_dir.resolve().parent != photos_dir.resolve()
        or not archive_dir.is_dir()
    ):
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    archive_worker = Path(__file__).with_name('archive_worker.py')
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(archive_worker),
        str(archive_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.get_event_loop().create_task(process.stderr.read())
    response = web.StreamResponse(
        headers={
            'Content-Type': 'application/zip',
            'Content-Disposition': (
                f'attachment; filename="photos-{archive_hash}.zip"'
            )
        },
    )
    download_interrupted = True
    try:
        await response.prepare(request)
        while True:
            chunk = await process.stdout.read(ARCHIVE_CHUNK_SIZE)
            if not chunk:
                break
            chunk_delay = request.app.get('archive_chunk_delay', 0)
            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)
            logger.debug('Sending archive chunk ... (%d bytes)', len(chunk))
            await response.write(chunk)

        return_code = await process.wait()
        error_output = await stderr_task
        if return_code:
            logger.error(
                'Archive worker failed for %s (exit code %d): %s',
                archive_hash,
                return_code,
                error_output.decode('utf-8', errors='replace').strip(),
            )
            raise RuntimeError(
                'Archive worker failed with exit code {}'.format(return_code)
            )

        await response.write_eof()
        download_interrupted = False
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        if not stderr_task.done():
            stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await stderr_task
        await process.communicate()
        if download_interrupted:
            logger.debug('Download was interrupted')
    return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r', encoding='utf-8') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def create_app(options):
    app = web.Application()
    app['photos_dir'] = options.photos_dir
    app['archive_chunk_delay'] = options.delay
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}', archive),
        web.get('/archive/{archive_hash}/', archive),
    ])
    return app


if __name__ == '__main__':
    options = parse_arguments()
    configure_logging(options.logging_enabled)
    app = create_app(options)
    web.run_app(app, shutdown_timeout=1)
