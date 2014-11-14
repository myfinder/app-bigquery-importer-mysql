# NAME

App::BigQuery::Importer::MySQL - BigQuery data importer from MySQL tables.

# SYNOPSIS

    $ cpanm App::BigQuery::Importer::MySQL
    $ mysqlbq --db_host localhost --src SCHEMA_NAME.TABLE_NAME --dst DATASET_NAME.TABLE_NAME

# DESCRIPTION

App::BigQuery::Importer::MySQL is BigQuery data importer from MySQL tables.

# REQUIRED COMMANDS

    mysql client cli
    gcloud cli

# REQUIRED FILES

    ~/.my.cnf
    ~/.bigqueryrc

# LICENSE

Copyright (C) Tatsuro Hisamori.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Tatsuro Hisamori <myfinder@cpan.org>
