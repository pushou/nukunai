#Usage:
# > nu kunai_kunai_print_events_table.nu 
# print table of kunai events


def print_events [] {
    kunai config --list-events
       | from ssv --noheaders 
       | insert events_name {each {$in.column0|str replace ':' ''}}
       | rename column1 events_id
       | move events_id --after events_name
       | reject column1 
}

def main [
] {
    print_events
}
