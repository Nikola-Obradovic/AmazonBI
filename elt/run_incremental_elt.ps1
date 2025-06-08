

# 1) Go to project root
Set-Location -Path 'C:\Users\User\PycharmProjects\AmazonBI'

# 2) Activate venv
& .\.venv\Scripts\Activate.ps1

# 3) Run the two incremental loads as modules
python -m elt.incremental_load_warehouse
python -m elt.incremental_load_star