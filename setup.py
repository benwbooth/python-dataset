from setuptools import setup

setup(name='dataset',
      version='0.1',
      description='sqldf-like ORM for python and PostgreSQL',
      url='https://github.com/benwbooth/python-dataset',
      author='Ben Booth',
      author_email='benwbooth@gmail.com',
      license='MIT',
      keywords="python dataset ORM SQL PostgreSQL postgres pandas sqlalchemy",
      zip_safe=True,
      py_modules=['dataset'],
      install_requires=['pandas','sqlalchemy','inflect','tabulate'])
