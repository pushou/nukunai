# nukunai — VHS demo of the nushell analysis queries (kunai_queries.nu)
# Render with:  vhs demo.tape
# Depends on:   ttyd + ffmpeg (VHS runtime deps)
Output nukunai-kunai-queries.gif

Set Shell bash
Set FontSize 12
Set Width 2400
Set Height 820
Set Padding 20
Set WindowBar Colorful
Set TypingSpeed 35ms
Set LoopOffset 10%

# Quiet setup: keep large nushell tables from opening the interactive pager.
Hide
Type "export PAGER=cat"
Enter
Sleep 300ms
Show

# -- 1. Start: the NGSOTI 22e4a57 sample (bare file -> default `events` view) --
Type "nu kunai_queries.nu ./logs/ngsoti/22e4a57ac560ebe1eff8957906589f4dd5934ee555ebcc0f7ba613b07fad2c13/kunai.jsonl"
Sleep 1s
Enter
Sleep 4s

# Keep the sample path handy for the rest of the tour.
Type "F=./logs/ngsoti/22e4a57ac560ebe1eff8957906589f4dd5934ee555ebcc0f7ba613b07fad2c13/kunai.jsonl"
Enter
Sleep 1s
Ctrl+L
Sleep 400ms

# -- 2. connect: destination IPs (public only) then destination ports --
Type "nu kunai_queries.nu connect-ips --public $F"
Sleep 700ms
Enter
Sleep 3s
Type "nu kunai_queries.nu connect-ports $F"
Sleep 700ms
Enter
Sleep 3s
Ctrl+L
Sleep 400ms

# -- 3. dns: queried domains --
Type "nu kunai_queries.nu dns $F"
Sleep 700ms
Enter
Sleep 3s
Ctrl+L
Sleep 400ms

# -- 4. iocs: consolidated compromise indicators --
Type "nu kunai_queries.nu iocs $F"
Sleep 700ms
Enter
Sleep 4s
Type "nu kunai_queries.nu iocs --public $F"
Sleep 700ms
Enter
Sleep 4s
