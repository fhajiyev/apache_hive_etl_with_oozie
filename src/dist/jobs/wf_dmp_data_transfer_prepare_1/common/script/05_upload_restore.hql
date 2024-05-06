USE SVC_DS_DMP;

DROP TABLE IF EXISTS prod_transfer_dataset_full;
ALTER TABLE prod_transfer_dataset_full_temp RENAME TO prod_transfer_dataset_full;

DROP TABLE IF EXISTS prod_transfer_dataset_diff;
ALTER TABLE prod_transfer_dataset_diff_temp RENAME TO prod_transfer_dataset_diff;

