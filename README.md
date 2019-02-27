# HTCondor

This profile configures Snakemake to run on clusters utilizing the
[HTCondor](https://research.cs.wisc.edu/htcondor/) scheduler.

## Setup

### Deploy profile

To deploy this profile, run

    mkdir -p ~/.config/snakemake
    cd ~/.config/snakemake
    cookiecutter https://github.com/msto/snakemake-condor-profile.git

Then, you can run Snakemake with

    snakemake --profile HTCondor ...

### Version support

Tested on HTCondor 8.4.11 and Snakemake 5.4.2 
