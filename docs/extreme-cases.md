# Extreme cases of the HPC controller

What the controller (`internal/`), the follower and the Python runner (`runner/`) do under pressure, and the
automated test that proves it. Source of intent: PRD #1461 (section 10 threats T-xx, decisions D-xx).

Status legend

| Status | Meaning |
|---|---|
| COVERED | proven by a test that existed before this document |
| ADDED | a test added with this document (alone or on top of existing proof) |
| FIXED | the new test found a bug; the fix is in the same change |
| OPEN | real gap found by reading the code; not fixed here, needs an owner decision (see "Open findings") |
| LIVE ONLY | cannot be proven with the fakes; the reason is given |

Test references are `file::name`. Go specs that are Ginkgo `It`s are cited by their text. `internal/controller`
specs run against envtest; the site is played by status writes (what the runner and the site poller would write).

## 1. Budget

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 1.1 | Share exhausted while jobs are queued | A job that fits the share but not the free part waits (`WaitingForBudget`); a job that can never fit this period is `Refused/BudgetBlocked`; a job already admitted is never touched (CON-05) | `internal/controller/extreme_test.go::should_refuse_new_work_once_the_share_is_spent_but_never_touch_a_job_already_admitted`; `hpcargoworkflowjob_controller_test.go::never overbooks: of five 4-hour jobs...`; `internal/admission/admission_test.go::TestDecide` | ADDED |
| 1.2 | The last node-minute | `cost == left` is admitted, `cost == left + 1` waits; freed minutes go to the waiting job | `extreme_test.go::should_admit_up_to_the_last_node_minute_and_wait_for_one_more`; `internal/ledger/ledger_extreme_test.go::TestShouldSpendTheLastNodeMinuteExactlyAndNotOne` | ADDED |
| 1.3 | N admissions racing for the last node-minutes | No over-booking and no double count. Admission is serial by construction (`MaxConcurrentReconciles: 1`, optimistic lock on every decision, bookings made in this process kept in `pending` until the cache shows them) | `extreme_test.go::should_never_overbook_a_burst_of_forty_jobs_through_a_lagging_cache_and_a_restart` (exact greedy outcome, never above the share); `hpcargoworkflowjob_controller_test.go::never overbooks when its own bookings are not visible yet (a lagging cache)`, `::fails a decision made on a stale copy of the job instead of applying it (optimistic lock)`; `ledger_test.go::TestPendingBookingsCountUntilTheCacheShowsThem`; `ledger_extreme_test.go::TestShouldNeverCountAJobTwiceWhenItIsBothPendingAndInTheCache`. Goroutine-parallel `Reconcile` calls are not a supported mode (one worker) and are not tested | ADDED |
| 1.4 | Status write fails after the booking was remembered | The failed booking is forgotten (no phantom booking, no metric); the retry books once | `extreme_test.go::should_book_once_when_the_status_write_fails_and_the_reconcile_is_retried` | ADDED |
| 1.5 | Garbage / `NO_VAL` / negative / future site times | Unknown start settles 0, unknown end settles to now, never negative, never absurd; ledger clamps old garbage records | `settle_test.go::TestSettle`; `hpcargoworkflowjob_controller_test.go::never settles a negative or absurd attempt: the live node-sharing garbage (2026-11-06)`; `ledger_test.go::TestComputeClampsGarbageAttempts`; `runner/tests/test_core.py::test_slurm_no_val_and_infinite_times_are_absent` | COVERED |
| 1.6 | Host idle tail (the site charges the node until the host job ends) | Settlement only ever grows from a valid, later end; repeating the report changes nothing; `NO_VAL` cannot extend it | `settle_test.go::TestLateEnd`; `hpcargoworkflowjob_controller_test.go::settles a host from its Slurm job's end when that is reported after its step (idempotently)`, `::never lowers a host's settlement and ignores an implausible late end (NO_VAL)`; `runner/tests/test_runner.py::test_a_host_reports_its_step_at_once_and_its_job_end_when_the_job_ends`, `::test_a_host_job_that_never_ends_is_given_up_after_its_limit_plus_margin` | COVERED |
| 1.7 | Ledger scale | Derived from the jobs every time; linear in the number of jobs | `ledger_extreme_test.go::TestShouldDeriveALedgerOfTwentyThousandJobsWithinASecond` | ADDED |
| 1.8 | Concurrency limit | Waits, never refused (D-49) | `hpcargoworkflowjob_controller_test.go::waits at the concurrency limit and is never refused for it (D-49)` | COVERED |

## 2. Site outage

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 2.1 | Slurm REST 5xx / 429 / timeout / connection reset / non-JSON 200 / 401 for a long time, job queued or running (runner) | The poll error is transient: no step failure, no second submit, no cancel, one terminal report when the site is back (300 polls = 2.5 h at 30 s, 16 combinations) | `runner/tests/test_extreme.py::test_should_ride_out_a_long_site_outage_without_failing_resubmitting_or_cancelling`; `test_runner.py::test_a_failing_state_poll_is_transient` | ADDED |
| 2.2 | The site never comes back | The runner has no give-up of its own. It is bounded by the runner Job's `activeDeadlineSeconds` (wall time + 8 h queue allowance + 30 min) and then `Failed/RunnerFailed` with `(DeadlineExceeded)` in the message and the run time settled. The site job is NOT cancelled by that path (finding F-1) | `test_extreme.py::test_should_not_end_a_run_by_itself_when_the_site_never_comes_back` (pins the contract); `extreme_test.go::should_name_the_deadline_when_the_runner_Job_ran_out_of_time_and_release_the_booking` | ADDED, OPEN (F-1) |
| 2.3 | Site job purged (REST and accounting answer 404) | The runner keeps asking, never submits another job for the attempt, never invents a terminal state; ends only through 2.2 (finding F-2: the message does not say the site lost the job) | `test_extreme.py::test_should_never_resubmit_or_report_over_a_site_job_the_site_has_purged`; poller side: `sitepoll_test.go::TestShouldRecordAJobTheSiteNoLongerListsAsEmptyStateSoTheRunnerAsksAccounting` | ADDED, OPEN (F-2) |
| 2.4 | The submit answer is lost (timeout after the site created the job) | The first runner does not retry in-process; the restarted pod adopts the job by key: one site job | `test_extreme.py::test_should_adopt_instead_of_resubmitting_when_the_submit_answer_was_lost`; `test_runner.py::test_a_restarted_runner_adopts_the_active_job_by_key` | ADDED |
| 2.5 | 5xx at submit | The runner stops with the site's reason in the pod termination message (`HTTP 503 ...`); the Job retries the pod (backoffLimit 4) and the next runner adopts anything the site did create | `test_extreme.py::test_should_stop_loudly_on_a_5xx_at_submit_so_the_pod_retry_adopts_by_key`; `test_runner.py::test_a_refused_submit_stops_the_runner_with_the_reason_and_reports_nothing` | ADDED |
| 2.6 | JWT expired / rotated mid-queue (runner) | The token is read fresh from the mounted file on every request; a 401 before rotation is transient | `test_core.py::test_the_token_is_read_fresh_on_every_request`; `test_extreme.py::test_should_use_the_rotated_jwt_on_the_very_next_request_after_a_401`, `::test_should_keep_a_queued_job_through_a_jwt_that_is_401_until_it_rotates` | ADDED |
| 2.7 | JWT rotated (site poller) | The Secret is read uncached each poll; the next poll uses the new token; no token, no call | `sitepoll_extreme_test.go::TestShouldUseTheRotatedJWTOnTheNextPollWithoutARestart`; `sitepoll_test.go::TestShouldNotCallTheSiteWithoutAToken`; `sitestate_test.go::TestShouldNotPutTheTokenInAnError` | ADDED |
| 2.8 | Site poller: 5xx, HTML-200 allow-list page, hung site | A failed poll changes no status; a hung site is cut at the interval; a failing chunk discards the partial picture; one site down does not stop the others; a deleted HPCSite is skipped | `sitepoll_test.go::TestShouldLeaveEveryStatusAloneWhenTheSiteFails`; `sitestate_test.go::TestShouldFailTheWholePollOnAnyBadReply`; `sitepoll_extreme_test.go::TestShouldKeepPollingTheOtherSitesWhenOneSiteIsDownOrItsHPCSiteIsGone`, `::TestShouldTreatAnHTMLPageWithHTTP200AsAFailedPoll`, `::TestShouldReturnWithinTheIntervalWhenTheSiteNeverAnswersAndLeaveEveryStatusAlone`; `sitestate_test.go::TestShouldFailTheWholePollWhenTheSiteNeverAnswersAndStopAtTheContext`, `::TestShouldDiscardEverythingWhenALaterChunkFails` | ADDED |
| 2.9 | Poller silent (controller down or site down) | After 3 intervals the runner polls the site itself, slowly; with no observation it waits 90 s first | `test_runner.py::test_a_stale_observation_means_the_controller_is_down_and_the_runner_polls_slowly`, `::test_without_any_observation_the_runner_waits_for_the_controller_then_polls_slowly` | COVERED |
| 2.10 | Stager SSH unreachable (login node down, ssh exit 255) | Dataset: retried with waits capped at 10 min for at most the 4 h staging budget, then the job stages in-job; the step is not failed. Image: one probe, the job pulls. The real `Stager` returns rc 255 quickly | `test_extreme.py::test_should_retry_an_unreachable_stager_with_bounded_waits_then_stage_in_job`, `::test_should_pull_the_image_in_job_when_the_stager_is_unreachable`, `::test_should_not_hang_or_raise_when_ssh_itself_cannot_connect`; `test_staging.py::test_retry_resumes_and_no_space_gives_up` | ADDED |
| 2.11 | Kubernetes API unreachable for the runner | State reports are best effort; the end report is retried 60 x 10 s, then the runner exits 1 and the pod retry adopts and reports again; the job read is retried 30 x 10 s | `runner/tests/test_reporter.py::test_the_end_of_an_attempt_is_reported_until_it_lands`, `::test_an_end_that_cannot_be_reported_fails_the_runner_so_the_job_retries`, `::test_a_state_report_that_fails_does_not_stop_the_runner`, `::test_the_job_is_read_through_api_blips_and_a_missing_one_is_none` | COVERED |
| 2.12 | A cancel meets a 5xx | The DELETE is retried with 2/4/8/16 s backoff (inside the pod's 60 s grace) and says so when it gives up (D-20) | `test_extreme.py::test_should_retry_a_cancel_the_site_answers_with_5xx_until_it_goes_through`, `::test_should_give_up_a_cancel_after_a_bounded_backoff_and_say_the_job_may_live`, `::test_should_not_retry_a_cancel_of_a_job_the_site_does_not_know` | FIXED (F-3, partly) |

## 3. Crashes

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 3.1 | Runner pod killed between submit and the status report | The next runner finds the active job by key (job name from workflow UID + step) or the job its predecessor reported; never a second submit; a job it cancelled for a resubmit and another attempt's job are never adopted | `test_runner.py::test_a_restarted_runner_adopts_the_active_job_by_key`, `::test_the_log_offset_of_the_adopted_job_is_where_the_stream_resumes`, `::test_a_job_that_ended_while_no_runner_watched_is_adopted_not_resubmitted`, `::test_a_job_this_runner_cancelled_for_a_resubmit_is_not_adopted`, `::test_another_attempts_job_is_never_adopted`, `::test_an_attempt_that_already_reported_its_end_does_nothing` | COVERED |
| 3.2 | SIGTERM without a cancel request (drain, eviction, node loss) | The site job is left alone, the log offset saved; with an unreadable job it still does not cancel | `test_runner.py::test_sigterm_without_a_cancel_request_leaves_the_site_job_for_the_next_runner`, `::test_sigterm_with_an_unreadable_job_does_not_cancel`, `::test_sigterm_saves_the_log_offset_for_the_next_runner`; `serve_test.go::never marks a cancel when only the runner pod is evicted (Job alive)` | COVERED |
| 3.3 | Runner pods fail and are replaced by the Job | The attempt is not failed while the Job lives; restarts are counted | `extreme_test.go::should_not_fail_the_attempt_while_the_runner_Job_replaces_failed_pods` | ADDED |
| 3.4 | Controller restart between two admissions | The ledger is derived from the jobs, not memory: earlier bookings still count | `extreme_test.go::should_keep_the_bookings_of_a_controller_that_restarted_between_two_admissions` and the restart in the forty-job burst (1.3) | ADDED |
| 3.5 | Controller restart mid-booking (status write lost) | See 1.4 | | ADDED |
| 3.6 | Controller restart while a site job is RUNNING | State is all in `status`; the runner Job and site job keep going | AC-04 lab test (PRD): "restart while a job is RUNNING" | LIVE ONLY (needs a real cluster and a running site job) |
| 3.7 | Follower restart | Re-attaches to the existing runner Job by its deterministic name, never creates a second, survives API blips, exits 1 and keeps the Job when stopped | `internal/follower/follower_test.go::TestShouldReattachToTheExistingJobWithoutCreatingAnother`, `::TestShouldSurviveAPIBlipsOnCreateAndGet`, `::TestShouldExitOneAndKeepTheJobWhenStopped`, `::TestShouldExitOneWhenTheJobDisappears`, `::TestShouldFailWhenTheExistingJobHasAnotherSpec` | COVERED |
| 3.8 | Shared-node host manager killed hard (SIGKILL, no TERM handler) | The guest stays held and marked taken; its own runner finds the dead host and starts over (4.2) | `runner/tests/test_extreme_slots.py::test_a_host_killed_hard_leaves_its_guest_held_for_its_runner_to_find` | ADDED |

## 4. Node sharing

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 4.1 | Host terminated cleanly (TERM) with guests running | Guests go back to the queue with the next attempt number, lease kept; a guest killed before the handler ran is requeued, not failed | `runner/tests/test_slot_manager.py::test_a_terminated_host_hands_its_guests_back_to_the_queue`, `::test_a_guest_killed_with_its_cancelled_host_is_requeued_not_failed` | COVERED |
| 4.2 | Host ended in any terminal state (`NODE_FAIL`, `CANCELLED`, `TIMEOUT`, `OUT_OF_MEMORY`, `PREEMPTED`, `FAILED`, `COMPLETED`) with the guest still held | The guest's runner cancels the held job once and resubmits it alone; alive states (`RUNNING`, `COMPLETING`, `SUSPENDED`, `PENDING`) and an unreadable host record never cause a resubmit | `test_extreme.py::test_should_start_a_taken_guest_over_when_its_host_ended_in_any_terminal_state`, `::test_should_leave_a_taken_guest_alone_while_its_host_is_alive`, `::test_should_leave_a_taken_guest_alone_when_the_host_record_cannot_be_read`; `test_core.py::test_a_host_is_gone_only_when_accounting_says_it_ended`; `test_runner.py::test_a_host_that_died_with_the_job_starts_it_over_bounded` | ADDED |
| 4.3 | Hosts keep dying | At most `LOST_HOST_RESUBMITS` (3) restarts, then the runner keeps watching the last job (no cancel/submit storm). It then waits for the runner Job deadline (finding F-4: no explicit reason) | `test_extreme.py::test_should_start_over_at_most_three_times_when_hosts_keep_dying` | ADDED, OPEN (F-4) |
| 4.4 | Settlement of a lost host | `NODE_FAIL` host settles from its reported run (to now when no end), not from the booking; booking released | `extreme_test.go::should_settle_a_host_lost_to_node_fail_from_its_reported_run`; `extreme_unit_test.go::TestSettleOfAHostLostToNodeFailChargesItsRunNotTheBooking`, `::TestOutcomeOfEverySiteEndStateOfAnAttempt` | ADDED |
| 4.5 | Guest taken, then host lost before the guest finished | While hosted the guest holds no booking (host covers the node) but counts as active; after the resubmit it holds its own booking again and settles by its own run; a guest that ended on its host settles 0; no double charge at any step | `extreme_test.go::should_book_a_guest_again_when_its_host_is_lost_and_charge_it_by_its_own_run`, `::should_charge_nothing_for_a_guest_that_ended_on_a_host_whose_node_then_failed`; `ledger_extreme_test.go::TestShouldBookAGuestAgainWhenItsHostIsLostAndKeepItActiveThroughout`; `hpcargoworkflowjob_controller_test.go::charges a guest on a shared node nothing: its host's booking covers the node (D-26)` | ADDED |
| 4.6 | Guest whose step limit exceeds the host's remaining time | Not taken, never held, read once and skipped for good (padded job limits do not matter; the step limit does) | `test_slot_manager.py::test_a_guest_fits_on_its_step_limit_not_its_padded_job_limit` | COVERED |
| 4.7 | 30-minute test-lane host | Takes only guests whose step ends 5 min before the host's end (15 min yes; 26, 60, 180 min no) | `test_extreme_slots.py::test_a_30_minute_test_lane_host_takes_only_the_guests_that_end_before_it_does`; `test_runner.py::test_a_shared_node_host_pads_its_limit_only_up_to_the_lane_maximum` | ADDED |
| 4.8 | Guest whose wallet ends before its step could | Not taken (D-07) | `test_extreme_slots.py::test_a_guest_whose_token_ends_before_its_step_could_is_not_taken` | ADDED |
| 4.9 | Memory hog, stopped guest, requeued attempt | Stopped alone; no result for a stopped run; claimed under the new attempt | `test_slot_manager.py::test_a_step_over_its_memory_share_is_stopped_alone`, `::test_stopping_a_guests_run_stops_only_its_step`, `::test_a_requeued_job_is_claimed_under_its_new_attempt` | COVERED |

## 5. Lanes

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 5.1 | One-job slot occupied by a job outside KAOS | The controller cannot see it; the site refuses the lane (QoS/reservation/limit texts) and the runner submits once more on the class QoS; the placement then shows the fallback and frees the lane | `test_runner.py::test_a_refused_lane_is_retried_once_on_the_class_default_without_a_reservation` (6 refusal texts), `::test_a_second_refusal_after_the_fallback_still_fails_loudly`, `::test_a_refusal_unrelated_to_the_lane_is_not_retried`; `lanes_test.go::should_show_the_class_default_and_free_the_lane_when_the_runner_reports_a_fallback` | COVERED |
| 5.2 | Reservation expired or removed between placement and submit | At placement an expired/missing reservation is never chosen; after placement the site's refusal takes the fallback of 5.1 | `internal/lanes/lanes_test.go::TestChoose` (`..._reservation_expired`, `..._not_in_the_profile`, `..._of_another_partition`); `test_runner.py` (same refusal texts, `Requested reservation is invalid`) | COVERED |
| 5.3 | QoS removed from the profile / account | The candidate is skipped; the job takes the class default | `lanes/lanes_test.go::TestChoose` (`should_skip_a_qos_the_account_does_not_have`); `extreme_test.go::should_never_place_a_job_on_a_lane_the_profile_no_longer_offers` | ADDED |
| 5.4 | Two HPCSites with the same site identity | One shared semaphore across projects; another identity has its own | `lanes_test.go::should_count_the_same_site_identity_across_projects_but_not_another_identity` | COVERED |
| 5.5 | Burst of jobs for one-job lanes with a lagging cache | Each of `test` and `dev` is handed out once; the rest take the class default | `extreme_test.go::should_hand_each_one_job_lane_out_once_in_a_burst_the_cache_cannot_see`; `lanes_test.go::should_place_two_parallel_short_jobs_in_test_and_dev_and_the_rest_on_the_class_default` | ADDED |
| 5.6 | Preemptible lane | Never chosen (a preempted host takes its guests down) | `lanes/lanes_test.go::TestChoose` (`should_never_use_a_qos_another_qos_preempts`) | COVERED |
| 5.7 | Lane freed when the attempt settles or a host takes the guest | Yes | `lanes_test.go::should_free_the_lane_when_the_attempt_settles`, `::should_free_the_test_slot_when_a_host_takes_the_guest_that_held_it` | COVERED |

## 6. Time limits

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 6.1 | Learned limit hit twice | One retry at the cap, then `Failed` | `serve_test.go::retries once at the declared limit when a learned limit times out, then fails as today on a second TIMEOUT`, `::caps the timeout retry at the spec's wall time and does not retry a declared limit`; `placement_test.go::TestTimeoutRetry` | COVERED |
| 6.2 | One huge outlier in the history | Ignored beyond the 95th percentile with enough runs; with three runs the slowest is the p95 and the cap wins | `timelimit_test.go::TestLearn` (`one_slow_run_of_20...`), `::TestLearnIgnoresOneHugeOutlierWhenThereAreEnoughRuns`, `::TestLearnWithAHugeOutlierAmongFewRunsFallsBackToTheCap` | ADDED |
| 6.3 | Zero, negative, future, epoch, `NO_VAL`, wrong-attempt run times | Not samples | `placement_test.go::TestRunTime`; `extreme_unit_test.go::TestRunTimeOfZeroLengthIsNotASampleOfTheStep` | FIXED (zero-length runs were accepted) |
| 6.4 | Any garbage history | The limit stays in `[15 min, cap]`, never negative, never above the cap; `Retry` never exceeds a cap | `timelimit_test.go::TestLearnNeverLeavesItsBoundsWhateverTheHistoryHolds`, `::TestRetryNeverExceedsAnyCapAndNeverShrinks` | ADDED |

## 7. Scale

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 7.1 | 300 non-terminal jobs on one site | 3 `jobs/state` calls of 100 ids per interval, each through the token bucket (1 call/s here: two waits of 1 s) | `sitepoll_extreme_test.go::TestShouldPollThreeHundredJobsWithThreeCallsOfAHundredThroughTheTokenBucket`; `sitestate_test.go::TestShouldChunkALongListAndPassEveryChunkThroughTheBucket` | ADDED |
| 7.2 | Many callers on the bucket | The rate is held under concurrency; one bucket per site profile | `sitestate_test.go::TestBucketShouldHoldTheRateWhenManyPollersWaitAtOnce`, `::TestBucketShouldAllowTheBurstThenSpaceCallsByTheRate`; `sitepoll_test.go::TestShouldShareOneBucketPerSiteProfileAcrossHPCSites` | ADDED |
| 7.3 | Lane semaphore under concurrent reconciles | One worker; every lane handed out at most `MaxJobsPerUser` times (5.5) | see 5.5 | ADDED |
| 7.4 | No O(N^2) in the ledger | 20 000 jobs + 500 pending bookings in well under a second | `ledger_extreme_test.go::TestShouldDeriveALedgerOfTwentyThousandJobsWithinASecond` | ADDED |

## 8. Idle watchdog

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 8.1 | GPU idle but the log keeps growing | No kill (also with a slow CPU data-loading phase) | `test_idlewatch.py::test_should_not_end_a_step_whose_log_keeps_growing_with_the_gpu_idle`; `test_extreme.py::test_should_spare_a_gpu_step_that_keeps_logging_while_the_gpu_is_idle` (100 min) | ADDED |
| 8.2 | Silent CPU phase inside a GPU step (T-24, accepted) | Killed when GPU idle and log silent for the window, whatever the CPU does: marker written, TERM then KILL | `test_extreme.py::test_should_end_a_gpu_step_whose_gpu_is_idle_in_a_silent_cpu_phase_T24` (a real spinning process) | ADDED |
| 8.3 | `idle_timeout=0` | No watchdog payload, nothing built | `test_runner.py::test_idle_window_0_sends_no_watchdog_program`; `test_idlewatch.py::test_idle_timeout_0_starts_no_watchdog`; `test_extreme.py::test_should_start_no_watchdog_for_a_zero_window` | COVERED |
| 8.4 | Unreadable GPU, metric-only log lines, half lines, truncated log | Not idle / not growth / growth only when changed | `test_idlewatch.py` (`a_sample_that_cannot_be_taken...`, `the_samplers_own_metric_lines...`, `an_unfinished_line...`) | COVERED |
| 8.5 | Idle kill accounting | `Failed/Idle`, event, metric, node-minutes saved, none for a guest | `hpcargoworkflowjob_controller_test.go::ends a step the idle watchdog stopped as Failed/Idle...`, `::saves no node-minutes for an idle guest on a shared node` | COVERED |

## 9. Lifecycle

| # | Case | Expected behaviour | Proof | Status |
|---|---|---|---|---|
| 9.1 | HPCSite deleted while a booked serve-mode job runs | The job is cancelled through its runner Job (the SIGTERM cancels the site job), the attempt is settled from its run, the booking released, finalizer dropped. Before: it was set `Refused` with the booking and the live site job left unsettled and unwatched | `extreme_test.go::should_cancel_the_site_job_and_settle_it_when_the_hpcsite_is_deleted` | FIXED (F-5) |
| 9.2 | Namespace loses its project label while a job runs | Same as 9.1 | `extreme_test.go::should_cancel_the_site_job_and_settle_it_when_the_namespace_loses_its_project_label` | FIXED (F-5) |
| 9.3 | No HPCSite for a job that was never booked | `Refused/NoSite`; nothing to cancel | `extreme_test.go::should_refuse_a_job_that_was_never_booked_when_its_hpcsite_is_gone`; `hpcargoworkflowjob_controller_test.go::refuses a job of a project with no site for its class` | ADDED |
| 9.4 | Workflow deleted / shut down / ended / re-created | The job is cancelled through the runner Job after the cancel marker is in the API | `serve_test.go::cancels through the runner Job when the owning Workflow ends` (3 entries), `::marks the cancel request in the API before it deletes the runner Job`, `::treats a Workflow with another UID as deleted`, `::cancels when the job itself is deleted, and then lets it go` | COVERED |
| 9.5 | Credentials rotated mid-job | Site JWT: read per request (2.6, 2.7). Wallet: minted again before outputs. Kubernetes SA token: re-read per call | `test_runner.py::test_a_long_job_gets_a_fresh_wallet_for_its_outputs`, `::test_a_queued_job_is_replaced_before_its_wallet_expires`, `::test_a_job_a_host_took_is_never_replaced_for_its_wallet`; `test_reporter.py::test_the_kube_client_rereads_its_projected_token` | COVERED |

## Open findings (not fixed here)

| ID | Severity | Finding |
|---|---|---|
| F-1 | Medium | When the runner Job itself fails (deadline wall + 8 h 30 min, or 5 failed pods) the controller marks the attempt `Failed/RunnerFailed`, settles the time seen so far and releases the booking, but nothing cancels the site job: the runner's SIGTERM handler cancels only on a cancel request, and the repository has no orphan sweep (AC-08). A job that queued longer than 8 h, or a long site/API outage, leaves a live site job that runs uncharged until its Slurm time limit. Needs a decision: extend the deadline with the queue, or have the controller start a cancel-only runner for a failed Job whose site job is known and not terminal. |
| F-2 | Low | A purged site job (404 in REST and accounting) is polled until F-1's deadline; the failure message does not say the site lost the job. A "not found N times in a row and not listed by the poller" rule could fail it early with a clear reason. |
| F-3 | Medium | D-20 says a cancel is retried until the site confirms `CANCELLED`. Only the DELETE retry (2.12) is fixed here. The controller still finalizes the cancel when the runner Job is gone, whether or not the runner could cancel the site job. Same remedy as F-1. |
| F-4 | Low | After `LOST_HOST_RESUBMITS` the runner keeps watching a job held by a dead host until the deadline, with no explicit reason. |
| F-5 | (fixed) | See 9.1 and 9.2. |
| F-7 | Low | `slots.py` registers a guest in `slots` only after it wrote `kaos-state=taken` and launched the step. A TERM in that window leaves the guest held and marked taken (the guest's runner recovers it through 4.2). It makes `test_slot_manager.py::test_a_terminated_host_hands_its_guests_back_to_the_queue` fail about 1 run in 10 (also on `main`). |
| F-6 | Low | A dead login node with a healthy Slurm API delays the submit by up to the 4 h staging budget before the in-job fallback (2.10). By design (cheaper than staging on GPU time), but long. |

## Running the suites

```
KUBEBUILDER_ASSETS=<repo>/bin/k8s/<version> go test $(go list ./... | grep -v test/e2e)
PYTHONDONTWRITEBYTECODE=1 pytest runner/tests
make lint
```
