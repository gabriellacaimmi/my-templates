result <- pageview %>%
  filter(date(time_stamp_utc) between as.Date('2022-07-15') and as.Date('2022-11-06'),
         content_barrier == 'trial') %>%
  left_join(conversion_visit %>%
              filter(date(order_dtm) between as.Date('2022-07-15') and as.Date('2022-11-06'),
                     is_b2c == TRUE),
            by = c("visit_id" = "conversion_visit_id")) %>%
  group_by(day = as.Date(time_stamp_utc)) %>%
  summarise(users = n_distinct(visitor_id),
            converters = n_distinct(user_guid)) %>%
  arrange(day)
