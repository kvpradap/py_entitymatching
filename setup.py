import subprocess
import sys
import os
# check if pip is installed. If not, raise an ImportError
PIP_INSTALLED = True

try:
    import pip
except ImportError:
    PIP_INSTALLED = False

if not PIP_INSTALLED:
    raise ImportError('pip is not installed.')

def install_and_import(package):
    import importlib
    try:
        importlib.import_module(package)
    except ImportError:
        pip.main(['install', package])
    finally:
        globals()[package] = importlib.import_module(package)

# check if setuptools is installed. If not, install setuptools
# automatically using pip.
install_and_import('setuptools')

from setuptools.command.build_ext import build_ext as _build_ext

from distutils import ccompiler, msvccompiler
from distutils.sysconfig import get_python_inc


## fix compiler and build options
COMPILE_OPTIONS = {
        'msvc': ['/0x', '/EHsc'],
        'mingw32': ['-O3', '-ffast-math', '-march=native'],
        'other': ['-O3', '-ffast-math', '-march=native']
}

LINK_OPTIONS = {
        'msvc': [],
        'mingw32': [],
        'other':[]
}

class build_ext_options:
    def build_options(self):
        for e in self.extensions:
            e.extra_compile_args += COMPILE_OPTIONS.get(
                self.compiler.compiler_type, COMPILE_OPTIONS['other'])
        for e in self.extensions:
            e.extra_link_args += LINK_OPTIONS.get(
                    self.compiler.compiler_type, LINK_OPTIONS['other'])

class build_ext(_build_ext, build_ext_options):
    def build_extensions(self):
        build_ext_options.build_options(self)
        _build_ext.build_extensions(self)

# class build_ext(_build_ext):
#     def build_extensions(self):
#         import pkg_resources
#         # numpy_incl = pkg_resources.resource_filename('numpy', 'core/include')
#         numpy_incl = pkg_resources.resource_filename('numpy', '')
#
#         for ext in self.extensions:
#             if (hasattr(ext, 'include_dirs') and
#                     not numpy_incl in ext.include_dirs):
#                 ext.include_dirs.append(numpy_incl)
#         _build_ext.build_extensions(self)

def generate_cython():
    cwd = os.path.abspath(os.path.dirname(__file__))
    print("Cythonizing sources")
    p = subprocess.call([sys.executable, os.path.join(cwd,
                                                      'build_tools',
                                                      'cythonize.py'),
                         'py_entitymatching'],
                        cwd=cwd)
    if p != 0:
        raise RuntimeError("Running cythonize failed!")


cmdclass = {"build_ext": build_ext}

if __name__ == "__main__":

    no_frills = (len(sys.argv) >= 2 and ('--help' in sys.argv[1:] or
                                         sys.argv[1] in ('--help-commands',
                                                         'egg_info', '--version',
                                                         'clean')))

    cwd = os.path.abspath(os.path.dirname(__file__))
    if not os.path.exists(os.path.join(cwd, 'PKG-INFO')) and not no_frills:
        # Generate Cython sources, unless building from source release
        generate_cython()

    # find packages to be included.
    packages = setuptools.find_packages()

    with open('README.rst') as f:
        LONG_DESCRIPTION = f.read()

    extensions = [setuptools.Extension("py_entitymatching.debugblocker.debugblocker_cython",
                                       ["py_entitymatching/debugblocker/debugblocker_cython.pyx",
                                        "py_entitymatching/debugblocker/TopPair.cpp",
                                        "py_entitymatching/debugblocker/PrefixEvent.cpp",
                                        "py_entitymatching/debugblocker/GenerateRecomLists.cpp",
                                        "py_entitymatching/debugblocker/TopkHeader.cpp",
                                        "py_entitymatching/debugblocker/OriginalTopkPlain.cpp",
                                           ], language='c++',
                                       include_dirs=[])
                 ]
    setuptools.setup(
        name='py_entitymatching',
        version='0.3.1',
        description='Python library for end to end Entity Matching.',
        long_description=LONG_DESCRIPTION,
        url='https://sites.google.com/site/anhaidgroup/projects/magellan/py_entitymatching',
        author='UW Magellan Team',
        author_email='uwmagellan@gmail.com',
        license='BSD',
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: Science/Research',
            'Intended Audience :: Education',
            'License :: OSI Approved :: BSD License',
            'Operating System :: POSIX',
            'Operating System :: Unix',
            'Operating System :: MacOS',
            'Operating System :: Microsoft :: Windows',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Topic :: Scientific/Engineering',
            'Topic :: Utilities',
            'Topic :: Software Development :: Libraries',
        ],
        packages=packages,
        install_requires=[
            'PyPrind',
            'py_stringsimjoin==0.3.0',
            # dependencies such as py_stringmatching, joblib, pyprind
            'cloudpickle >= 0.2.1',
            'pyparsing >= 2.1.4',
            'scikit-learn >= 0.18',
            'pandas-profiling >= 1.4.0',
            'dask',
            'toolz'
        ],
        ext_modules=extensions,
        cmdclass=cmdclass,
        include_package_data=True,
        zip_safe=False
    )
