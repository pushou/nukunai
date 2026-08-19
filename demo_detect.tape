# nukunai — VHS demo: compromise detection on the NGSOTI 22e4a57 sample (perfctl miner)
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

# ----------------------------------------------------------------------------
# 1. Run the detection engine (9 families + ioc) on the sample.
# ----------------------------------------------------------------------------
Type "nu kunai_detect_compromise.nu ./logs/ngsoti/22e4a57ac560ebe1eff8957906589f4dd5934ee555ebcc0f7ba613b07fad2c13/kunai.jsonl"
Sleep 1s
Enter
Sleep 7s
Ctrl+L
Sleep 400ms

# ----------------------------------------------------------------------------
# 2. INTRUSION FOCUS — cat the markdown report: the execve section up top
#    shows the drop-and-run killchain (sample.bin -> oom_reaper -> perfctl
#    -> cron persistence perfcc), a perfctl crypto-miner compromise.
# ----------------------------------------------------------------------------
Type "R=$(ls -td logs/ngsoti/scanresult* | head -1)"
Enter
Sleep 1s
Type "cat $R/*.md"
Sleep 800ms
Enter
Sleep 5s
Ctrl+L
Sleep 400ms

# ----------------------------------------------------------------------------
# 3. The same intrusion, from the detections parquet via polars.
# ----------------------------------------------------------------------------
Type `nu -c "plugin use polars; polars open $R/detections_execve.parquet | polars collect"`
Sleep 700ms
Enter
Sleep 5s
