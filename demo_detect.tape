# nukunai — VHS demo of the compromise detection engine (kunai_detect_compromise.nu)
# Render with:  vhs demo_detect.tape
# Depends on:   ttyd + ffmpeg (VHS runtime deps)
Output images/detect.gif

Set Shell bash
Set FontSize 15
Set Width 2900
Set Height 1100
Set Padding 20
Set WindowBar Colorful
Set TypingSpeed 35ms
Set LoopOffset 10%

# Quiet setup: keep large tables from opening the interactive pager.
Hide
Type "export PAGER=cat"
Enter
Sleep 300ms
Show

# -- 1. Run the detection engine over the NGSOTI 22e4a57 sample (all families) --
Type "nu kunai_detect_compromise.nu ./logs/ngsoti/22e4a57ac560ebe1eff8957906589f4dd5934ee555ebcc0f7ba613b07fad2c13/kunai.jsonl"
Sleep 1s
Enter
Sleep 7s

# Keep the sample path handy, and grab the report dir the engine just wrote.
Type "F=./logs/ngsoti/22e4a57ac560ebe1eff8957906589f4dd5934ee555ebcc0f7ba613b07fad2c13/kunai.jsonl"
Enter
Sleep 1s
Type "R=$(ls -td logs/ngsoti/scanresult* | head -1)"
Enter
Sleep 1s
Ctrl+L
Sleep 400ms

# -- 2. Focus a single family (execve -> drop-and-run from /tmp) --
Type "nu kunai_detect_compromise.nu -f execve $F"
Sleep 700ms
Enter
Sleep 6s
Ctrl+L
Sleep 400ms

# -- 3. Show the persistent command lines captured in the markdown report --
Type "sed -n '/^## execve/,/^## file_create/p' $R/*.md"
Sleep 700ms
Enter
Sleep 3s
