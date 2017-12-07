use strict;
use warnings;
use Data::Dumper;
use URL::Encode;

# Webtrends Fields:
		# WT.vtid = user_id
		# WT.sr = screen resolution
		# WT.ti = page title
		# WT.js = javascript enabled
		# WT.bs = viewport size
		# WT.es = page url
		# WT.cg_n = page category
		# colltype = data source
		# place = ditto
		# rdata = other archiove ref
		# docref = docref

# load the sensitive data (logfile paths) from a config file which won't be uploaded to github
my %config = do("config.pl");
my $logfile_path = $config{'logfile_path'};
$logfile_path =~ s|\\|/|g;

# select some log files for processing
# Use a filter to restrict the files we process (start with a single file, and once the script is working, relax the filter to read in more data)
my $files_for_processing_filter = "*9h9r*2017-11*.log";
my @files_for_processing = glob "$logfile_path/$files_for_processing_filter";

# store all the data in the following hash
my $data;

# this hash is to confirm the hypothesis that the webtrends ID, stripped of the IP address, is still unique
my $serial_num = 0;
my $serial_num_for_ip;
my %series_found;

# for each log file
foreach my $filename (@files_for_processing) {
	print "Processing $filename\n";
	# open the file and read it in line by line
	open my $ifh, "<", $filename;
	while (my $line = <$ifh>) {
		next if $line =~ /^#/;	# skip lines that start with a # (comments)
		chomp $line;			# remove the trailing carriage return
		my @tokens = split / /, $line; # create an array of each of the columns
		next if ($tokens[4] ne 'discovery.nationalarchives.gov.uk');	# skip any non discovery entries
		
		# initially we're only interested in search tokens and IA details pages
		if ($tokens[6] !~ m|/results/r|i && $tokens[6] !~ m|/details/|i) {
			#print "$tokens[6]\n";
			next;
		}
		
		# extract the info from the Webtrends string into a %fields hash
		my $webtrends_string = $tokens[7];
		$webtrends_string =~ s/^&//;
		my @fields = split /&/, $webtrends_string;
		my %fields;
		map { $fields{ (split /=/)[0] } = (split /=/)[1];  } @fields;

		# for the moment, skip anything with a colltype (A2A, NRA etc)
		if (defined $fields{colltype}) {
			next;
		}
		
		# some entries record server errors. Skip them.
		if (defined $fields{"WT.ti"} && ($fields{"WT.ti"} =~ /^Sorry,%20there%20has%20been/
				|| $fields{"WT.ti"} =~ /Sorry,%20we%20can%E2%80%99t%20find%20the%20page/) ) {
			next;
		}
		
		# for some reason, some records don't have the Webtrends vtid field
		if (!defined $fields{"WT.vtid"}) {
			next;
#			if (defined $fields{"WT.vt_sid"}) {
#				$fields{"WT.vtid"} = $fields{"WT.vt_sid"};
#			}
#			else {
#				undef;
#			}
		}
		
		# gather the data for the webtrends ID check here
		my $id = $fields{"WT.vtid"};
		if ($id =~ /^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})-(\d+\.\d+)$/) {
			my ($ip,$id_part) = ($1,$2);
			if (!defined $ip || !defined $id_part) {
				undef;
			}
			my $anon_ip;
			if (!exists($serial_num_for_ip->{$ip})) {
				$serial_num_for_ip->{$ip} = $serial_num;
				$anon_ip = $serial_num;
				$serial_num++;
			}
			else {
				$anon_ip = $serial_num_for_ip->{$ip};
			}
			$id = "$anon_ip.$id_part";
		}
		# if it's a search results page, we need to record the search query (in an array of search queries)
		# the search query is stored in an attribute called _q or _aq
		if ($tokens[6] =~ m|/results/r|i) {
			my $query;
			if (defined $fields{_q}) {
				$query = $fields{_q};
			}
			elsif (defined $fields{_aq}) {
				$query = $fields{_aq};
			}
			else {
				next;
			}
			$query = URL::Encode::url_decode($query);
			$data->{ $id }{most_recent_search} = $query; # keep track of the most recent search term, to relate back any subsequent IA views
			$data->{ $id }{searches}{$query}{search_count}++; # keep track of the search queries (this could be an array but I'm using a hash to automatically de-dupe)
		}
		
		# if it's a details page, get the series reference
		if ($tokens[6] =~ m|/details/r|i) {
			my $docref = $fields{"docref"};
			my $series_ref;
			if (defined $docref) {
				$series_ref = get_series_ref($docref);
			}
			else {
				print "\$docref not defined for ".$fields{"WT.ti"}."\n";
			}
			if (defined $series_ref) {
				$series_found{$series_ref} = 1; # keep a list of unique series references - we'll need them to generate an array shortly
				# if we don't have an entry for this webtrends ID yet, this is the first clicked series, so let's record this
				if (!defined($data->{ $id })) {
					$data->{ $id }{first_series} = $series_ref;
				}
				# if there's a recent search term, log any IA views against it. Otherwise just log them without a search term (might still be useful to learn related series)
				if (defined($data->{ $id }{"most_recent_search"})) {
					my $mrs = $data->{ $id }{"most_recent_search"};
					$data->{ $id }{searches}{$mrs}{series_opened}{$series_ref}++;
					if (!defined $data->{ $id }{searches}{$mrs}{first_series}) {
						$data->{ $id }{searches}{$mrs}{first_series} = $series_ref;
					}
				}
				else {
					$data->{ $id }{series_opened}{$series_ref}++;
				}
			}
		}
	}
	close $ifh;
#	foreach my $id (keys %$data) {
#		print "ID = $id\n";
#		print Dumper $data->{$id};
#		print "\n";
#	}
}

# generate column positions for the series array in the output data
#my $column_for;
#my $col_num = 0;
#foreach my $series (sort keys %series_found) {
#	$column_for->{$series} = $col_num++;
#} 

open my $ofh, ">", "data_for_ml.txt";
print $ofh "ID\tSearchQuery\tFirst Series\t";
foreach my $series (sort keys %series_found) {
	print $ofh "$series\t";
}
print $ofh "\n";

foreach my $id (keys %$data) {
	foreach my $search_query (keys %{$data->{$id}{searches}}) {
		if (defined $data->{$id}{searches}{$search_query}{series_opened}) {
			print $ofh "$id\t$search_query\t$data->{$id}{searches}{$search_query}{first_series}\t";
			foreach my $series (sort keys %series_found) {
				if (defined $data->{$id}{searches}{$search_query}{series_opened}{$series}) {
					print $ofh $data->{$id}{searches}{$search_query}{series_opened}{$series};
				}
				print $ofh "\t";
			}
			print $ofh "\n";
		}
	}
}
close $ofh;

sub get_series_ref {
	my ($docref) = @_;
	$docref = URL::Encode::url_decode($docref); 
	$docref =~ m|([A-Z]+ \d+)|i;
	return $1;
}