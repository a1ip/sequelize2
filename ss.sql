SELECT project AS project_name, slug AS form_template_slug, form_template_name, interval,
    MIN(begin_date) start_date,
    MAX(end_date) end_date,
    SUM(not_done_overdue) not_done_overdue,
    SUM(pending_now) pending_now,
    SUM(pending_future) pending_future,
    SUM(done_early) done_early,
    SUM(done_late) done_late,
    SUM(done_ontime) done_ontime,
    SUM(total) total,
    project_id
FROM (
    -- *** THE QUERY PENDING GROUPING AND AGGREGATION  *** --
    WITH the_projects AS (
        SELECT DISTINCT P.id, P.name, P.created_at, P.state, P.is_federal, P.bmp_module_enabled, P.spcc_enabled, P.fire_extinguisher_inspection_enabled
        FROM main.projects P
            LEFT JOIN main.users_roles_projects URP
                ON P.id=URP.project_id
            LEFT JOIN main.users_roles_organizations URO
                ON P.organization_id=URO.organization_id
        WHERE P.is_archived=FALSE
            AND P.organization_id = 'ea1da92c-a926-43f9-9fb0-54f9f3f311c9'                     -- Variable here
            AND (URP.user_id = '05b0457b-7e6f-4b5b-b7c3-c6ec16240b52'                          -- Variable here
                OR URO.user_id = '05b0457b-7e6f-4b5b-b7c3-c6ec16240b52')                      -- Variable here
            AND P.id = uuid(COALESCE(NULLIF('',''),P.id::text)::text) -- Variable here
    ), the_dates AS (
        SELECT the_date FROM (
        SELECT TO_DATE('2017-12-25T12:13:15+03:00','yyyy-MM-dd')+(A.digit + (10  B.digit) + (100  C.digit) + (1000 * D.digit)) AS the_date        -- Variable here
        FROM (SELECT 0 AS digit UNION ALL SELECT 1 AS digit UNION ALL SELECT 2 AS digit UNION ALL SELECT 3 AS digit UNION ALL
                SELECT 4 AS digit UNION ALL SELECT 5 AS digit UNION ALL SELECT 6 AS digit UNION ALL SELECT 7 AS digit UNION ALL
                SELECT 8 AS digit UNION ALL SELECT 9 AS digit) A,
                (SELECT 0 AS digit UNION ALL SELECT 1 AS digit UNION ALL SELECT 2 AS digit UNION ALL SELECT 3 AS digit UNION ALL
                SELECT 4 AS digit UNION ALL SELECT 5 AS digit UNION ALL SELECT 6 AS digit UNION ALL SELECT 7 AS digit UNION ALL
                SELECT 8 AS digit UNION ALL SELECT 9 AS digit) B,
                (SELECT 0 AS digit UNION ALL SELECT 1 AS digit UNION ALL SELECT 2 AS digit UNION ALL SELECT 3 AS digit UNION ALL
                SELECT 4 AS digit UNION ALL SELECT 5 AS digit UNION ALL SELECT 6 AS digit UNION ALL SELECT 7 AS digit UNION ALL
                SELECT 8 AS digit UNION ALL SELECT 9 AS digit) C,
                (SELECT 0 AS digit UNION ALL SELECT 1 AS digit UNION ALL SELECT 2 AS digit UNION ALL SELECT 3 AS digit UNION ALL
                SELECT 4 AS digit UNION ALL SELECT 5 AS digit UNION ALL SELECT 6 AS digit UNION ALL SELECT 7 AS digit UNION ALL
                SELECT 8 AS digit UNION ALL SELECT 9 AS digit) D
            ) one_thousand_days
        WHERE the_date>=TO_DATE('2017-12-25T12:13:15+03:00','yyyy-MM-dd') AND the_date<=TO_DATE('2018-12-25T21:00:00Z','yyyy-MM-dd')  -- Variables here (2)
    )
    SELECT inspection.project_id, inspection.project, inspection.slug, inspection.interval, inspection.form_template_name,
        schedule.name AS schedule_name, schedule.begin_date, schedule.end_date, submission.id,
        CASE WHEN submission.id IS NULL
            AND schedule.end_date<CURRENT_DATE THEN 1 ELSE 0 END AS not_done_overdue,
        CASE WHEN submission.id IS NULL
            AND schedule.end_date>=CURRENT_DATE
            AND schedule.begin_date<=CURRENT_DATE THEN 1 ELSE 0 END AS pending_now,
        CASE WHEN submission.id IS NULL
            AND schedule.end_date>=CURRENT_DATE
            AND schedule.begin_date>CURRENT_DATE THEN 1 ELSE 0 END AS pending_future,
        CASE WHEN submission.id IS NOT NULL
            AND submission.assessment_date<schedule.begin_date THEN 1 ELSE 0 END AS done_early,
        CASE WHEN submission.id IS NOT NULL
            AND submission.assessment_date>schedule.end_date THEN 1 ELSE 0 END AS done_late,
        CASE WHEN submission.id IS NOT NULL
            AND submission.assessment_date<=schedule.end_date
            AND submission.assessment_date>=schedule.begin_date THEN 1 ELSE 0 END AS done_ontime,
        1 AS total
    FROM (
        -- CONFIGURED INSPECTIONS --
        SELECT DISTINCT TP.id AS project_id, TP.name AS project, ft.slug,
            ft.name as form_template_name, interv.interval,
            ft.id AS form_template_id, TP.created_at AS project_created_at,
            COALESCE(ft.deprecated_at, TO_DATE('2018-12-25T21:00:00Z','yyyy-MM-dd')) as available_until      --  Variable here
        FROM main.form_templates ft
            JOIN (
                (SELECT project_id, interval, interv3.form_template_slug
                FROM main.form_list_interval_settings interv3
                    JOIN (
                            SELECT MAX(swppp_start_year) AS active_year, form_template_slug
                            FROM main.form_list_interval_settings interv2
                                JOIN the_projects TP
                                    ON TP.id = interv2.project_id
                            GROUP BY form_template_slug
                        ) current_interv_year
                        ON current_interv_year.active_year = interv3.swppp_start_year
                            AND current_interv_year.form_template_slug = interv3.form_template_slug)
                UNION ALL
                (SELECT NULL AS project_id, MAX(B.interval) AS "interval", B.form_template_slug
                FROM main.form_list_interval_defaults B
                GROUP BY B.form_template_slug)
            ) interv ON interv.form_template_slug=ft.slug
            JOIN the_projects TP
                ON (TP.id = interv.project_id AND interv.project_id IS NOT NULL)
                    OR (
                        interv.project_id IS NULL
                        AND (
                            interv.form_template_slug = TP.state||'-routine-facility-inspection'
                            OR interv.form_template_slug = TP.state||'-visual-assessment'
                            OR (
                                TP.is_federal
                                AND (
                                    interv.form_template_slug IN ('federal-routine-facility-inspection','federal-visual-assessment')
                                    )
                                )
                            OR (
                                TP.bmp_module_enabled
                                AND interv.form_template_slug = 'bmp-routine-facility-inspection'
                                )
                            OR (
                                TP.spcc_enabled
                                AND interv.form_template_slug IN ('federal-industrial-spcc-regular-inspection','federal-industrial-spcc-frequent-inspection')
                                )
                            OR (
                                TP.fire_extinguisher_inspection_enabled
                                AND interv.form_template_slug = 'federal-fire-extinguisher'
                                )
                            )
                        )
    ) inspection
        JOIN (
            -- INSPECTION SCHEDULE: day,week, quarter, month, biannual, year --
            SELECT name, interval, begin_date, end_date
            FROM (
                SELECT the_date::text AS name,'day' AS interval,
                    the_date::DATE AS begin_date, the_date AS end_date
                FROM the_dates UNION ALL
                SELECT to_char(begin_date,'yyyy "Week "WW') AS name,'week' AS interval,
                    begin_date, end_date
                FROM (
                        SELECT MIN(the_date) AS begin_date, MAX(the_date) AS end_date
                        FROM the_dates
                        WHERE (EXTRACT(MONTH FROM the_date)>1 OR EXTRACT(WEEK FROM the_date)<45)
                            AND (EXTRACT(MONTH FROM the_date)<12 OR EXTRACT(WEEK FROM the_date)>45)
                        GROUP BY EXTRACT(YEAR FROM the_date), EXTRACT(WEEK FROM the_date)
                    ) the_weeks UNION ALL
                SELECT to_char(begin_date, 'yyyy Month') AS name,'month' AS interval,
                    begin_date, end_date
                FROM (
                        SELECT MIN(the_date) AS begin_date, MAX(the_date) AS end_date
                        FROM the_dates
                        GROUP BY EXTRACT(YEAR FROM the_date), EXTRACT(MONTH FROM the_date)
                    ) the_months UNION ALL
                SELECT to_char(end_date, 'yyyy "Quarter "Q') AS name,'quarter' AS interval,
                    begin_date, end_date
                FROM (
                        SELECT MIN(the_date) AS begin_date, MAX(the_date) AS end_date
                        FROM the_dates
                        GROUP BY EXTRACT(QUARTER FROM the_date),EXTRACT(YEAR FROM the_date)
                    ) the_quarters UNION ALL
                SELECT to_char(begin_date, 'yyyy Mon')||to_char(end_date, '-Mon" Half Year"') AS name,'biannual' AS interval,
                    begin_date, end_date
                FROM (
                        SELECT MIN(the_date) AS begin_date, MAX(the_date) AS end_date
                        FROM the_dates
                        GROUP BY CASE WHEN EXTRACT(MONTH FROM the_date)>6 THEN 2 ELSE 1 END,EXTRACT(YEAR FROM the_date)
                    ) the_biannual UNION ALL
                SELECT to_char(end_date, 'yyyy') AS name,'year' AS interval,
                    begin_date, end_date
                FROM (
                        SELECT MIN(the_date) AS begin_date, MAX(the_date) AS end_date
                        FROM the_dates
                        GROUP BY EXTRACT(YEAR FROM the_date)
                    ) the_years
                ) the_interval_entries
            WHERE begin_date >= TO_DATE('2017-12-26T12:13:15+03:00','yyyy-MM-dd') -- Variable Here
            ORDER BY interval, begin_date
        ) schedule
            ON schedule.interval=inspection.interval
                AND schedule.begin_date <= inspection.available_until
        LEFT JOIN (
            -- SUBMITTED INSPECTIONS --
            SELECT FTS.id, FTS.marked_completed_by_user, FTS.form_template_id, FTS.project_id,
                TO_DATE(RIGHT(FTS.slug,11), 'yyyy-mon-dd') AS inspection_date,
                TO_DATE(FTSF.field_value,'yyyy-MM-dd') AS assessment_date
            FROM main.form_template_submissions FTS
            JOIN main.form_template_submission_fields FTSF
                ON FTS.id=FTSF.form_template_submission_id
            JOIN the_projects TP
                ON TP.id = FTS.project_id
            WHERE FTSF.field_slug IN ('info-date','inspect-date')
                AND LEFT(RIGHT(FTS.slug,3),1)='-'
                AND LEFT(RIGHT(FTS.slug,7),1)='-'
                AND SUBSTRING(FTSF.field_value,5,1)='-'
                AND SUBSTRING(FTSF.field_value,8,1)='-'
                AND TO_DATE(RIGHT(FTS.slug,11), 'yyyy-mon-dd')>=TO_DATE('2017-12-26T12:13:15+03:00','yyyy-MM-dd') -- Variable Here
                AND TO_DATE(RIGHT(FTS.slug,11), 'yyyy-mon-dd')<TO_DATE('2018-12-25T21:00:00Z','yyyy-MM-dd')+1  -- Variable Here
            ORDER BY TO_DATE(RIGHT(FTS.slug,11), 'yyyy-mon-dd')
        ) submission
            ON submission.form_template_id=inspection.form_template_id
                AND submission.project_id=inspection.project_id
                AND submission.inspection_date>=schedule.begin_date
                AND submission.inspection_date<=schedule.end_date
    WHERE schedule.end_date>inspection.project_created_at
        OR submission.id IS NOT NULL
) stats
GROUP BY project_id, project, slug, "interval", form_template_name
ORDER BY project, form_template_name, "interval";