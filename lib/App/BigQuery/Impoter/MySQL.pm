package App::BigQuery::Impoter::MySQL;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01_1";



1;
__END__

=encoding utf-8

=head1 NAME

App::BigQuery::Impoter::MySQL - BigQuery data importer from MySQL tables.

=head1 SYNOPSIS

    $ cpanm App::BigQuery::Impoter::MySQL
    $ mysqlbq -m ~/.my.cnf -b ~/.bigqueryrc --src SCHEMA_NAME.TABLE_NAME --dest DATASET_NAME.TABLE_NAME

=head1 DESCRIPTION

App::BigQuery::Impoter::MySQL is BigQuery data importer from MySQL tables.

=head1 LICENSE

Copyright (C) Tatsuro Hisamori.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Tatsuro Hisamori E<lt>myfinder@cpan.orgE<gt>

=cut
