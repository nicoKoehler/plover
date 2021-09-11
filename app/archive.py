import threading as th
def udf_progressBar(step, total, steps, t0):
    incr = int(total/steps)
    prgr = (i-i%incr)/incr
    rem = steps-prgr
    print(f"Progress: {(i/total):.0%}  [{'█' * int(prgr)}{'-' * int(rem)}] @{(time.time() - t0):.1f}sec ", end="\r")

# ++++++++++++++++++++++ REPEAT FUNCTION ++++++++++++++++++++++
def udf_repeat():
    t = th.Timer(5.0, udf_repeat)
    t.daemon = True #exits timer when no non-daemon threads are alive anymore
    t.start()
    print(f"Min. Data Collection: {len(df_d10)} entries {(len(df_d10)/(iLookBack)):.0%} in {(time.time() - t0):.2f} seconds", end="\r")
    