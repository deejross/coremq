"""
CoreMQ, a pure-Python message queue and WebSocket server based on asyncio.

source: https://github.com/deejross/coremq
author: Ross Peoples (http://www.rosspeoples.com/)
"""

try:
    from setuptools import setup
except ImportError:
    from disutils.core import setup
import os

CURRENT_DIR = os.path.dirname(__file__)

setup(
    author='Ross Peoples',
    author_email='ross.peoples@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4'
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries',
    ],
    keywords='message queue web socket websocket websockets',
    description='Message queue and WebSocket server implementation using asyncio for Python 2.7+ and 3.4+',
    download_url='https://github.com/deejross/coremq/archive/master.tar.gz',
    install_requires=[
        'trollius',
        'autobahn[asyncio]'
    ],
    license='MIT',
    long_description=open(os.path.join(CURRENT_DIR, 'README.rst')).read(),
    name='coremq',
    packages=['coremq'],
    url='https://github.com/deejross/coremq/',
    version='1.0.0'
)
