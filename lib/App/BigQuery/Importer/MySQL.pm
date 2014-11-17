package App::BigQuery::Importer::MySQL;
use 5.008001;
use strict;
use warnings;
use Getopt::Long qw(:config posix_default no_ignore_case gnu_compat);
use File::Temp qw(tempfile tempdir);
use File::Basename;
use DBI;
use Carp qw(croak);

our $VERSION = "0.02";

sub new {
    my ($class, $args) = @_;

    my @required_list = qw/ src dst mysqlhost mysqluser mysqlpassword project_id progs /;
    for my $required (@required_list) {
        if( ! defined $args->{$required} ) { croak "$required is required"};
    }

    bless {
        dryrun        => $args->{dryrun},
        src           => $args->{src},
        dst           => $args->{dst},
        mysqlhost     => $args->{mysqlhost},
        mysqluser     => $args->{mysqluser},
        mysqlpassword => $args->{mysqlpassword},
        project_id    => $args->{project_id},
        progs         => $args->{progs},
    }, $class;
}

sub run {
    my $self = shift;

    my $db_host                  = $self->{'mysqlhost'};
    my ($src_schema, $src_table) = split /\./, $self->{'src'};
    my ($dst_schema, $dst_table) = split /\./, $self->{'dst'};

    # check the table does not have BLOB or TEXT
    my $dbh = DBI->connect("dbi:mysql:${src_schema}:${db_host}", $self->{'mysqluser'}, $self->{'mysqlpassword'});
    my $blob_text_check_sql = "SELECT SUM(IF((DATA_TYPE LIKE '%blob%' OR DATA_TYPE LIKE '%text%'),1, 0)) AS cnt
        FROM INFORMATION_SCHEMA.columns
        WHERE TABLE_SCHEMA = '${src_schema}' AND TABLE_NAME = '${src_table}'";
    my $cnt = $dbh->selectrow_hashref($blob_text_check_sql);
    if ($cnt->{cnt} > 0) {
        die "${src_schema}.${src_table} has BLOB or TEXT table";
    }

    # create BigQuery schema json structure
    my $schema_type_check_sql = "SELECT
        CONCAT('{\"name\": \"', COLUMN_NAME, '\",\"type\":\"', IF(DATA_TYPE LIKE \"%int%\", \"INTEGER\",IF(DATA_TYPE = \"decimal\",\"FLOAT\",\"STRING\")) , '\"}') AS json
        FROM INFORMATION_SCHEMA.columns where TABLE_SCHEMA = '${src_schema}' AND TABLE_NAME = '${src_table}'";
    my $rows = $dbh->selectall_arrayref($schema_type_check_sql);
    my @schemas;
    for my $row (@$rows) {
        push @schemas, @$row[0];
    }
    my $bq_schema_json = '[' . join(',', @schemas) . ']';
    my($bq_schema_json_fh, $bq_schema_json_filename) = tempfile;
    unless ($self->{'dryrun'}) {
        print {$bq_schema_json_fh} $bq_schema_json;
    }

    # create temporary bucket
    my $bucket_name = $src_table . '_' . time;
    unless ($self->{'dryrun'}) {
        my $mb_command = "$self->{'progs'}->{'gsutil'} mb -p $self->{'project_id'} gs://$bucket_name";
        my $result_create_bucket = system($mb_command);
        if ($result_create_bucket != 0) {
            die "${mb_command} : failed";
        }
    }

    # dump table data
    my $dump_command = "$self->{'progs'}->{'mysql'} -u$self->{'mysqluser'} -p'$self->{'mysqlpassword'}' -h$self->{'mysqlhost'} ${src_schema} -Bse'SELECT * FROM ${src_table}'";
    my $dump_result = `$dump_command`;
    if ($? != 0) {
        die "${dump_command} : failed";
    }
    $dump_result =~ s/\"//g;
    $dump_result =~ s/NULL//g;
    my($src_dump_fh, $src_dump_filename) = tempfile;
    unless ($self->{'dryrun'}) {
        print {$src_dump_fh} $dump_result;
    }

    # upload dump data
    my $dump_upload_command = "$self->{'progs'}->{'gsutil'} cp $src_dump_filename gs://$bucket_name";
    unless ($self->{'dryrun'}) {
        my $result_upload_schema = system($dump_upload_command);
        if ($result_upload_schema != 0) {
            die "${dump_upload_command} : failed";
        }
    }

    # copy to BigQuery
    my $remove_table_command = "$self->{'progs'}->{'bq'} rm -f $self->{'dst'}";
    my $src_dump_file_basename = basename($src_dump_filename);
    unless ($self->{'dryrun'}) {
        my $result_remove_table = system($remove_table_command);
        if ($result_remove_table != 0) {
            die "${remove_table_command} : failed";
        }
        my $load_dump_command = "$self->{'progs'}->{'bq'} load -F '\\t' --max_bad_record=300 $self->{'dst'} gs://${bucket_name}/${src_dump_file_basename} ${bq_schema_json_filename}";
        my $result_load_dump = system($load_dump_command);
        if ($result_load_dump != 0) {
            die "${load_dump_command} : failed";
        }
    }

    # remove dump data
    my $dump_rm_command = "$self->{'progs'}->{'gsutil'} rm gs://${bucket_name}/${src_dump_file_basename}";
    unless ($self->{'dryrun'}) {
        my $result_dump_rm = system($dump_rm_command);
        if ($result_dump_rm != 0) {
            die "${dump_rm_command} : failed";
        }
    }

    # remove bucket
    my $bucket_rm_command = "$self->{'progs'}->{'gsutil'} rb -f gs://$bucket_name";
    unless ($self->{'dryrun'}) {
        my $result_bucket_rm = system($bucket_rm_command);
        if ($result_bucket_rm != 0) {
            die "${dump_rm_command} : failed";
        }
    }
}

1;
__END__

=encoding utf-8

=head1 NAME

App::BigQuery::Importer::MySQL - BigQuery data importer from MySQL tables.

=head1 SYNOPSIS

    use App::BigQuery::Importer::MySQL

=head1 DESCRIPTION

App::BigQuery::Importer::MySQL is BigQuery data importer from MySQL tables.

B<THE SOFTWARE IS ALPHA QUALITY. API MAY CHANGE WITHOUT NOTICE.>

=head1 REQUIRED COMMANDS

    mysql client cli
    gcloud cli

=head1 REQUIRED FILES

    ~/.my.cnf
    ~/.bigqueryrc

=head1 LICENSE

Copyright (C) Tatsuro Hisamori.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Tatsuro Hisamori E<lt>myfinder@cpan.orgE<gt>

=cut
