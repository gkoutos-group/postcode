import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
        name='integrator',
        version='0.1',
        author='Victor Roth Cardoso',
        author_email='labrax@gmail.com',
        description='Increment your dataset with associated external variables',
        long_description=long_description,
        long_description_content_type='text/markdown',
        url='https://github.com/gkoutos_group/postcode',
        packages=setup.find_packages(),
        classifies=['Programming Language :: Python :: 3', 'Operating System :: OS Independent'],
)

