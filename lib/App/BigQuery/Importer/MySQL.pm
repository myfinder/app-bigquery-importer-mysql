package App::BigQuery::Importer::MySQL;
use 5.008001;
use strict;
use warnings;
use Getopt::Long qw(:config posix_default no_ignore_case gnu_compat);
use Config::CmdRC ( file => ['.my.cnf', '.bigqueryrc'], );
use DBI;
use Data::Dumper;

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

    print Dumper $self;
    my $db_host = $self->opt->{db_host};
    my $src     = $self->opt->{src};
    my ($src_schema, $src_table) = split /\./, $src;
    my $dst     = $self->opt->{dst};
    my $project = $self->project_id;

    # check the table does not have BLOB or TEXT
    my $check_sql = "SELECT SUM(IF((DATA_TYPE LIKE '%blob%' OR DATA_TYPE LIKE '%text%'),1, 0)) AS cnt
        FROM INFORMATION_SCHEMA.columns
        WHERE TABLE_SCHEMA = '$src_schema' AND TABLE_NAME = '$src_table'";

        #my $dbh = DBI->connect("dbi:mysql:${src_schema}:${db_host}", $self->mysqluser, $self->mysqlpassword);
        #my $rs  = $dbh->selectall_arrayref($sql);
        #print Dumper $rs;
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
