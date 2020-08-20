from setuptools import setup

setup(name='pco_rclient',
      version='0.4.3',
      description="Rest client script for the pco writer.",
      author='Paul Scherrer Institute (PSI)',
      maintainer="Paul Scherrer Institute",
      maintainer_email="daq@psi.ch",
      url='https://github.com/paulscherrerinstitute/pco_rclient',
      license="GPL3",
      packages=['pco_rclient',
                'pco_rclient.client'],
      include_package_data=True
      )
