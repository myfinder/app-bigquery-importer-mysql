package App::BigQuery::Importer::MySQL;
use 5.008001;
use strict;
use warnings;
use Getopt::Long qw(:config posix_default no_ignore_case gnu_compat);
use Config::CmdRC ( file => ['.my.cnf', '.bigqueryrc'], );
use DBI;
use File::Temp qw(tempfile tempdir);
use Time::Piece;
use File::Basename;

use Class::Accessor::Lite (
    new => 1,
    rw  => [ qw(
        dryrun
        opt
        mysqluser
        mysqlpassword
        project_id
        progs
    ) ],
);

our $VERSION = "0.01_1";

sub _get_options {
    my $self = shift;
    my %opt;
    GetOptions(
        \%opt,
        qw(db_host=s src=s dst=s dryrun)
    ) or $self->usage;

    my @required_options = qw(db_host src dst);
    $self->usage if grep {!exists $opt{$_}} @required_options;

    if ($opt{dryrun}) {
        $self->dryrun(1);
    }
    $self->opt(\%opt);
}

sub _check_config {
    my $self = shift;

    defined RC->{'default.project_id'} or $self->usage;
    defined RC->{'client.user'} or $self->usage;
    defined RC->{'client.password'} or $self->usage;

    $self->project_id(RC->{'default.project_id'});
    $self->mysqluser(RC->{'client.user'});
    $self->mysqlpassword(RC->{'client.password'});
}

sub _check_prog {
    my $self = shift;

    my $pathes = +{};
    my @progs = qw(mysql gsutil bq);
    for my $prog (@progs) {
       my $path = `which $prog 2> /dev/null` or $self->usage;
       chomp $path if $path;
       $pathes->{$prog} = $path;
    }
    $self->progs($pathes);
}

sub usage {
    my $self = shift;

    my $message = <<'USAGE';
mysqlbq - command description
  Usage: command [options]
  Options:
    --db_host   MySQL Hostname or IP addr(ex: localhost)
    --src       MySQL Schema and Table name(ex: schema_name.table_name)
    --dst       BigQuery Dataset and Table name(ex: dataset_name.table_name)
    --dryrun    dry run mode
    -h(--help)  show this help
  Requirement Programs: mysql cli and gcloud package
  Requirement Files: this script needs ~/.my.cnf and ~/.bigqueryrc files
    ~/.my.cnf:
      [client]
      user = user
      password = pass
    ~/.bigqueryrc:
      project_id = pj_id
      credential_file = /path/to/credential.json
USAGE

    print $message;
    exit 1;
}

sub run {
    my $self = shift;
    $self->_check_prog;
    $self->_check_config;
    $self->_get_options;
    $self->main;
}

sub main {
    my $self = shift;

    my $db_host                  = $self->opt->{db_host};
    my $src                      = $self->opt->{src};
    my ($src_schema, $src_table) = split /\./, $src;
    my $dst                      = $self->opt->{dst};
    my ($dst_schema, $dst_table) = split /\./, $dst;
    my $project                  = $self->project_id;

    # check the table does not have BLOB or TEXT
    my $dbh = DBI->connect("dbi:mysql:${src_schema}:${db_host}", $self->mysqluser, $self->mysqlpassword);
    my $blob_text_check_sql = "SELECT SUM(IF((DATA_TYPE LIKE '%blob%' OR DATA_TYPE LIKE '%text%'),1, 0)) AS cnt
        FROM INFORMATION_SCHEMA.columns
        WHERE TABLE_SCHEMA = '${src_schema}' AND TABLE_NAME = '${src_table}'";
    my $cnt = $dbh->selectrow_hashref($blob_text_check_sql);
    if ($cnt->{cnt} > 0) {
        warn "${src_schema}.${src_table} has BLOB or TEXT table"; exit 1;
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
    print {$bq_schema_json_fh} $bq_schema_json;

    # create temporary bucket
    my $bucket_name = $src_table . '_' . localtime->epoch;
    my $mb_command = "$self->{'progs'}->{'gsutil'} mb -p $self->{'project_id'} gs://$bucket_name";
    my $result_create_bucket = system($mb_command);
    if ($result_create_bucket != 0) {
        warn "${mb_command} : failed"; exit 1;
    }

    # dump table data
    my($src_dump_fh, $src_dump_filename) = tempfile;
    my $dump_command = "$self->{'progs'}->{'mysql'} -u$self->{'mysqluser'} -p'$self->{'mysqlpassword'}' -h$self->{'opt'}->{'db_host'} ${src_schema} -Bse'SELECT * FROM ${src_table}'";
    my $dump_result = `$dump_command`;
    if ($? != 0) {
        warn "${dump_command} : failed"; exit 1;
    }
    $dump_result =~ s/\"//g;
    $dump_result =~ s/NULL//g;
    print {$src_dump_fh} $dump_result;

    # upload dump data
    my $dump_upload_command = "$self->{'progs'}->{'gsutil'} cp $src_dump_filename gs://$bucket_name";
    my $result_upload_schema = system($dump_upload_command);
    if ($result_upload_schema != 0) {
        warn "${dump_upload_command} : failed"; exit 1;
    }

    # copy to BigQuery
    my $remove_table_command = "$self->{'progs'}->{'bq'} rm -f $dst";
    my $result_remove_table = system($remove_table_command);
    if ($result_remove_table != 0) {
        warn "${remove_table_command} : failed"; exit 1;
    }
    my $src_dump_file_basename = basename($src_dump_filename);
    my $load_dump_command = "$self->{'progs'}->{'bq'} load -F '\\t' --max_bad_record=300 $dst gs://${bucket_name}/${src_dump_file_basename} ${bq_schema_json_filename}";
    my $result_load_dump = system($load_dump_command);
    if ($result_load_dump != 0) {
        warn "${load_dump_command} : failed"; exit 1;
    }

    # remove dump data
    my $dump_rm_command = "$self->{'progs'}->{'gsutil'} rm gs://${bucket_name}/${src_dump_file_basename}";
    my $result_dump_rm = system($dump_rm_command);
    if ($result_dump_rm != 0) {
        warn "${dump_rm_command} : failed"; exit 1;
    }

    # remove bucket
    my $bucket_rm_command = "$self->{'progs'}->{'gsutil'} rb -f gs://$bucket_name";
    my $result_bucket_rm = system($bucket_rm_command);
    if ($result_bucket_rm != 0) {
        warn "${dump_rm_command} : failed"; exit 1;
    }

    print "finish import job!!\n";
}

1;
__END__

=encoding utf-8

=head1 NAME

App::BigQuery::Importer::MySQL - BigQuery data importer from MySQL tables.

=head1 SYNOPSIS

    $ cpanm App::BigQuery::Importer::MySQL
    $ mysqlbq --db_host localhost --src SCHEMA_NAME.TABLE_NAME --dst DATASET_NAME.TABLE_NAME

=head1 DESCRIPTION

App::BigQuery::Importer::MySQL is BigQuery data importer from MySQL tables.

=head1 REQUIREMENTS

    mysql client cli
    gcloud cli

=head1 LICENSE

Copyright (C) Tatsuro Hisamori.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Tatsuro Hisamori E<lt>myfinder@cpan.orgE<gt>

=cut
