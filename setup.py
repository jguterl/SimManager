"""
"""

# Always prefer setuptools over distutils
from setuptools import setup,find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.
print('=== Setup SimulationManager Package ===')
setup(
    name='SimManager',
    version='0.2',
    setup_requires=[],
    description='Utility to launcvh and monitor simulations locally or remotely with slurm',
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jguterl/SimulationManager/',
    author='J. Guterl',  # Optional

    # This should be a valid email address corresponding to the author listed
    # above.
    author_email='guterlj@fusion.gat.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        #'Intended Audience :: Any UEDGE users',
        'Topic :: Software Development',
        #'License :: ???',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='sample, setuptools, development',  # Optional
    #packages=['UEDGEToolBox'],
    python_requires='>=3.5, <4',
    install_requires=[],


    project_urls={
    },
)
