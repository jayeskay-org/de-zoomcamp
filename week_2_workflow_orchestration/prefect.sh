#!/usr/bin/env bash

run_prefect () {
    prefect server start
}


install_prefect () {
    pip install prefect
}

{
  install_prefect && run_prefect
} ||
{
  run_prefect
}
 
