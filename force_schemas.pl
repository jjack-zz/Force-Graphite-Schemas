#!/usr/bin/perl

use strict;
use warnings;

my $VERSION = '0.01';

use Data::Dumper;
use File::Find qw(find);
use Getopt::Long;
use YAML;

my $GRAPHITE_BASE   = "/opt/graphite";
my $STORAGE_SCHEMAS = "conf/storage-schemas.conf";
my $STORAGE_PATH    = "storage/whisper/";
my $WHISPER_INFO    = "/usr/bin/whisper-info.py";
my $WHISPER_RESIZE  = "/usr/bin/whisper-resize.py";

die "$GRAPHITE_BASE/$STORAGE_SCHEMAS not found!\n"
    unless ( -e "$GRAPHITE_BASE/$STORAGE_SCHEMAS" );

my ( $reallyrun, $help, $noop ) = ( 0, 0, 0 );
GetOptions(
    "r|reallyrun" => \$reallyrun,
    "h|help"      => \$help,
    "n|noop"      => \$noop,
);

usage() and exit(0) if ($help || !($reallyrun || $noop));

# take the current schemas flle and ...
open my $SCHEMAS_FILE, "$GRAPHITE_BASE/$STORAGE_SCHEMAS"
    or die "Unable to open: $!\n";
my $schemas_string = join "", <$SCHEMAS_FILE>;
close $SCHEMAS_FILE;

# ... convert it to yaml because writing a parser was overkill
$schemas_string =~ s/\[([\w-]+)\]/ - name: $1/gm;
foreach (qw( priority pattern retentions )) {
    $schemas_string =~ s/($_) =/   $1:/gm;
    $schemas_string =~ s/$_: (.+)/$_: "$1"/gm;
}

# an array here because the actual order is very important to us
my @retentions = YAML::Load($schemas_string);

# recursively go over the whisper files
# and update them according to the schemas
my $dir = "$GRAPHITE_BASE/$STORAGE_PATH";
find(
    sub {
        if (/\.wsp$/) {
            my $full_path     = $File::Find::name;
            my $graphite_name = graphitify($full_path);

            foreach my $retentionos (@retentions) {

                # we only want the first matched type
                my ($match) = grep { $graphite_name =~ /$_->{pattern}/ } @$retentionos;

                next unless ( $match->{name} );

                # does not currently support retentions values like "60:43200,900:350400"
                # only works with "60:43200"
                # TODO: add this
                next unless ( $match->{retentions} =~ /^\d+:\d+$/ );

                my $current_retention = get_retention($full_path);

                if ( $current_retention ne $match->{retentions} ) {
                    printf "Name:\t\t%s\n",        $graphite_name;
                    printf "Matched:\t\%s (%s)\n", $match->{name}, $match->{pattern};
                    printf "Current:\t%s\n",       $current_retention;
                    printf "Proposed:\t%s\n",      $match->{retentions};
                    if ($reallyrun) {
                        system( "$WHISPER_RESIZE $full_path $match->{retentions}" );
                        `chown carbon:carbon $full_path`;
                        `chown carbon:carbon $full_path.bak`;
                        print "\n";
                    }
                    else {
                        printf "Would Run:\t%s %s %s\n\n", $WHISPER_RESIZE,
                            $full_path, $match->{retentions};
                    }
                }
            }
        }
    },
    $dir
);

sub graphitify {

    # from: /opt/graphite/storage/whisper/server/name/type/datapoint.wsp
    # to:   server.name.type.datapoint

    my $path = shift;
    $path =~ /^$GRAPHITE_BASE\/$STORAGE_PATH(.+)\.wsp$/;
    $path = $1;
    $path =~ s/\//\./g;

    return $path;
}

sub get_retention {
    my $path   = shift;
    my $output = `$WHISPER_INFO $path`;
    $output =~ /secondsPerPoint: (\d+)\npoints: (\d+)/;

    return "$1:$2";
}

sub usage {
    print <<'USAGE';
Force Schemas - simple tool to enforce current retention policies and schemas on your Graphite installation

Usage:
   force_schemas.pl [options]

   options are as follows :
   -h, --help             : display this help message
   -r, --reallyrun        : actually process and modify whisper databases
   -n, --noop             : default- runs in noop mode, showing you the commands that would be run

USAGE
    exit 0;
}

=pod

=head1 NAME

Force Schemas

=head1 DESCRIPTION

A simple tool to enforce current retention policies and schemas 
for your Graphite installation.

High Level Overview:
- Grab all schema info as set in $GRAPHITE_BASE/$STORGE_SCHEMAS
- Run through all Whisper databases as defined in $GRAPHITE_BASE/$STORAGE_PATH
- Run /usr/bin/whisper-info.py against them
- Match the database's name with the storage schema
- If the schema is up to date, nothing happens, otheriwse it runs /usr/bin/whisper-resize.py against it

NOTE: /usr/bin/whisper-resize.py creates a .bak file. After you've verified that your data has copied
      over correctly, be sure to delete all of them!

=head1 USAGE

force_schemas.pl [options]

=over

=item -h, --help : display this help message
=item -r, --reallyrun : actually process and modify whisper databases
=item -p, --path : the path where to create your application
=item -n, --noop : default; runs in noop mode, showing you the commands that would be run

=back

=head1 EXAMPLE

Here is an example of it being --reallyrun
$ force_schemas.pl --reallyrun
Name:		server.name.type.datapoint_a
Matched:	everything_1min_3_years (.*)
Current:	60:12345
Proposed:	60:1577846
Retrieving all data from the archives
Creating new whisper database: /opt/graphite/storage/whisper/server/name/type/datapoint_a.wsp.tmp
Created: /opt/graphite/storage/whisper/server/name/type/datapoint_a.wsp.tmp (18934180 bytes)
Migrating data...
Renaming old database to: /opt/graphite/storage/whisper/server/name/type/datapoint_a.wsp.bak
Renaming new database to: /opt/graphite/storage/whisper/server/name/type/datapoint_a.wsp

Name:		server.name.type.datapoint_b
Matched:	everything_1min_3_years (.*)
Current:	60:12345
Proposed:	60:1577846
Retrieving all data from the archives
Creating new whisper database: /opt/graphite/storage/whisper/server/name/type/datapoint_b.wsp.tmp
Created: /opt/graphite/storage/whisper/server/name/type/datapoint_b.wsp.tmp (18934180 bytes)
Migrating data...
Renaming old database to: /opt/graphite/storage/whisper/server/name/type/datapoint_b.wsp.bak
Renaming new database to: /opt/graphite/storage/whisper/server/name/type/datapoint_b.wsp


And an example of it being run with no options or --noop
$ force_schemas.pl --noop
Name:		server.name.type.datapoint_a
Matched:	everything_1min_3_years (.*)
Current:	60:12345
Proposed:	60:1577846
Would Run:	/usr/bin/whisper-resize.py /opt/graphite/storage/whisper/server/name/type/datapoint_a.wsp 60:1577846

Name:		server.name.type.datapoint_b
Matched:	everything_1min_3_years (.*)
Current:	60:12345
Proposed:	60:1577846
Would Run:	/usr/bin/whisper-resize.py /opt/graphite/storage/whisper/server/name/type/datapoint_b.wsp 60:1577846

=head1 AUTHOR

This script was written by Jeremy Jack<jjack@mediatemple.net>

=head1 LICENSE

This program is free software and is published under the same
terms as Perl itself.

=cut
