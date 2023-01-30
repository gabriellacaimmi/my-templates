with p2 as (
        select
        a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , m.messaging as shown_banner --value is cookieConsentC
        from `ft-customer-analytics.wp_gcaimmi.registration_test_RR` as a ---table storing test data (variation name, visitor_id, device_spoor_id, device_type, user_guid)
        LEFT JOIN `ft-data.spoor.on_site_messaging` m ON m.device_spoor_id = a.device_spoor_id AND DATE(m._PARTITIONTIME) >= '2022-12-05' and lower(messaging) like '%cookie%'
        group by 1,2,3,4,5
      )

      , p3 as (
        select
        a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , m.messaging_action --value is accept & continue
        from p2 as a
        LEFT JOIN `ft-data.spoor.on_site_messaging` m ON m.device_spoor_id = a.device_spoor_id AND DATE(m._PARTITIONTIME) >= '2022-12-05' and lower(messaging) like '%cookie%'
        AND messaging_action ='Accept & continue'
        -- AND a.month=DATE_TRUNC(DATE(m.time_stamp), MONTH)
        group by 1,2,3,4,5,6
      )

      , p4 as (
        select
        DISTINCT a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , pr.preference_status AS cookies_consent
        from p3 as a
        LEFT JOIN `ft-bi-team.reporting_tables.fpd_historical_prefs` pr ON pr.ft_user_id = a.user_guid AND a.user_guid IS NOT NULL AND a.user_guid <> '' AND pr.month_beginning = '2023-01-01'
        AND pr.permission_type = 'Cookies'
        GROUP BY 1,2,3,4,5,6,7
      )

      , p5 as (
        select
        DISTINCT a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , pr.preference_status AS email_marketing_consent --enabled = yes
        from p4 as a
        LEFT JOIN `ft-bi-team.reporting_tables.fpd_historical_prefs` pr ON pr.ft_user_id = a.user_guid AND a.user_guid IS NOT NULL AND a.user_guid <> '' AND pr.month_beginning = '2023-01-01'
        AND pr.permission_type = 'Email Marketing'
        GROUP BY 1,2,3,4,5,6,7,8
      )

      ,p6 as ( -- this subquery can potentially be removed at a later date
        select
        DISTINCT a.optimizely_variations_name 
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , a.email_marketing_consent --enabled = yes
        , pr.preference_status AS email_enhanced_marketing_consent --enabled = yes
        from p5 as a
        LEFT JOIN `ft-bi-team.reporting_tables.fpd_historical_prefs` pr ON pr.ft_user_id = a.user_guid AND a.user_guid IS NOT NULL AND a.user_guid <> '' AND pr.month_beginning = '2023-01-01'
        AND pr.permission_type = 'Enhancement Marketing'
        GROUP BY 1,2,3,4,5,6,7,8,9
      )

      , p7 as (
        select 
        DISTINCT a.optimizely_variations_name 
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , a.email_marketing_consent --enabled = yes
        , a.email_enhanced_marketing_consent --enabled = yes
        , CASE WHEN (k.user_cohort_primary is null or k.user_cohort_primary = '') then 'anonymous'
        ELSE k.user_cohort_primary
        END as primary_cohort
        , k.user_industry
        , k.user_responsibility
        , k.user_position
        from p6 as a
        LEFT JOIN `ft-bi-team.BI_Layer_Integration.known_user_daily_status` as k
        ON k.user_guid = a.user_guid
        and user_status_date = '2023-01-04'
        AND user_status_date >= '2022-12-05' 
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13
      )

      , p8 as (
        select a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , a.email_marketing_consent --enabled = yes
        , a.email_enhanced_marketing_consent --enabled = yes
        , a.primary_cohort
        , a.user_industry
        , a.user_responsibility
        , a.user_position
        , case
        when lower(user_industry) in ('n/a', '','unknown','not specified') or user_industry is null then 0
        else 1
        end as user_industry_specified
        , case
        when lower(user_responsibility) in ('n/a', '','unknown','not specified') or user_responsibility is null then 0
        else 1
        end as user_responsibility_specified
        , case
        when lower(user_position) in ('n/a', '','unknown','not specified','other') or user_position is null then 0
        else 1
        end as user_position_specified
        from p7 as a
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
      )

      , p9 as (
        select a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , case
        when cookies_consent = 'Enabled' then 'cookie_consent'
        else 'no_cookie_consent'
        end as stored_cookie_consent_status
        , case
        when a.shown_banner = 'cookieConsentC' and a.messaging_action ='Accept & continue' then 'cookie_consent'
        else 'no_cookie_consent'
        end as banner_cookie_consent_status
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , a.email_marketing_consent --enabled = yes
        , a.email_enhanced_marketing_consent --enabled = yes
        , a.primary_cohort
        , a.user_industry
        , a.user_responsibility
        , a.user_position
        , a.user_industry_specified
        , a.user_responsibility_specified
        , a.user_position_specified
        , sum(a.user_industry_specified + a.user_responsibility_specified + a.user_position_specified) as demo_data_points
        from p8 as a
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
      )

      , p10 as (
        select a.optimizely_variations_name
        , a.device_spoor_id
        , a.device_type
        , a.user_guid
        , a.stored_cookie_consent_status
        , a.banner_cookie_consent_status
        ,case
        when a.stored_cookie_consent_status = 'cookie_consent' or a.banner_cookie_consent_status = 'cookie_consent' then 'consented_to_cookies'
        else 'not_consented_to_cookies'
        end as cookie_consent
        , a.shown_banner --value is cookieConsentC
        , a.messaging_action --value is accept & continue
        , a.cookies_consent --enabled = yes
        , a.email_marketing_consent --enabled = yes
        , a.email_enhanced_marketing_consent --enabled = yes
        , a.primary_cohort
        , a.user_industry
        , a.user_responsibility
        , a.user_position
        , a.user_industry_specified
        , a.user_responsibility_specified
        , a.user_position_specified
        , sum(a.user_industry_specified + a.user_responsibility_specified + a.user_position_specified) as demo_data_points
        from p9 as a
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
      )


      select a.optimizely_variations_name
      , a.device_spoor_id
      , a.device_type
      , a.user_guid
      , a.stored_cookie_consent_status
      , a.banner_cookie_consent_status
      , a.cookie_consent
      , a.messaging_action
      , a.email_marketing_consent --enabled = yes
      , a.email_enhanced_marketing_consent --enabled = yes
      , a.primary_cohort
      , a.demo_data_points
      , case
      when cookie_consent = 'consented_to_cookies' and email_marketing_consent = 'Enabled' and demo_data_points >=3 then 'Recognised'
      else 'Unrecognised'
      End as user_recognised
      from p10 as a
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13
