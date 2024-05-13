set export

# Ejecutar test (Omitiendo los lentos)
test *args='.': clean
    python -m pytest -m "not slow" -x --failed-first {{ args }}


# Borrar ficheros compilados de python
clean:
    sudo find . -type d -name "__pycache__" -exec rm -rf "{}" +
    sudo find . -type d -name ".mypy_cache" -exec rm -rf "{}" +
    sudo find . -type f -name "*.pyc" -delete
    sudo find . -type f -name "*.pyo" -delete

# Ejecutar ctags
tags:
    cd {{justfile_directory()}} && ctags -R

migrate:
    ./madorx migrate all --num-days 21
