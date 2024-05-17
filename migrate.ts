#!/usr/bin/env bun
import { Database, constants } from "bun:sqlite";

if (Bun.argv.length !== 3) {
  console.log("Usage:");
  console.log("./migrate.ts file.db");
  console.log();
  process.exit(Bun.argv.length === 2 ? 0 : 1);
}

const path = Bun.argv.at(-1)!;

if (!await Bun.file(path).exists()) {
  console.log(`File does not exist: ${path}`);
  process.exit(1);
}

const millisInDay = 24 * 60 * 60_000;

const TABLE = "TimeSeries";

// https://github.com/chartium/dashboard/blob/main/src/lib/overview/datasetNames.ts
export enum Dataset {
  HP_CAPACITY_SUBSCRIBED_SUM = "SUBSCRIBED_CAPACITY",
  HP_CAPACITY_LOGICAL_SUM = "LOGICAL_CAPACITY",
  HP_CAPACITY_PHYSICAL_SUM = "PHYSICAL_CAPACITY",

  EMC_CAPACITY_SUBSCRIBED_SUM = "SUBSCRIBED_CAPACITY_EMC",
  EMC_CAPACITY_NET_SUM = "NET_CAPACITY_EMC",
  EMC_CAPACITY_PHYSICAL_SUM = "PHYSICAL_CAPACITY_EMC",

  HP_CAPACITY_PHYSICAL_PERSYSTEM = "CAPACITY",
  HP_WORKLOAD_PERSYSTEM = "WORKLOAD",
  HP_TRANSFER_PERSYSTEM = "TRANSFER",

  EMC_CAPACITY_PHYSICAL_PERSYSTEM = "CAPACITY_PERSYSTEM_EMC",
  EMC_WORKLOAD_PERSYSTEM = "WORKLOAD_PERSYSTEM_EMC",
  EMC_TRANSFER_PERSYSTEM = "TRANSFER_PERSYSTEM_EMC",

  HP_BLOCKSIZE_READ_AVG = "BLOCKSIZE_READ",
  HP_BLOCKSIZE_WRITE_AVG = "BLOCKSIZE_WRITE",

  HP_WORKLOAD_SUM = "WORKLOAD_SUM",
  HP_TRANSFER_SUM = "TRANSFER_SUM",

  EMC_WORKLOAD_SUM = "WORKLOAD_EMC",
  EMC_TRANSFER_SUM = "TRANSFER_EMC",
}

const correspondingMetrics: [persystem: Dataset, sum: Dataset][] = [
  [Dataset.HP_WORKLOAD_PERSYSTEM, Dataset.HP_WORKLOAD_SUM],
  [Dataset.HP_TRANSFER_PERSYSTEM, Dataset.HP_TRANSFER_SUM],
  [Dataset.EMC_WORKLOAD_PERSYSTEM, Dataset.EMC_WORKLOAD_SUM],
  [Dataset.EMC_TRANSFER_PERSYSTEM, Dataset.EMC_TRANSFER_SUM],
];

const whereDatasetIs = (dataset: string) =>
  `WHERE dataset = ${dataset} AND variant = 'default'`;

const whereDatasetsOverlap = (persystem: string, sum: string) =>
  `${whereDatasetIs(persystem)} AND x IN (SELECT x FROM ${TABLE} ${whereDatasetIs(sum)
  })`;

const whereDatasetsDontOverlap = (persystem: string, sum: string) =>
  `${whereDatasetIs(persystem)} AND x NOT IN (SELECT x FROM ${TABLE} ${whereDatasetIs(sum)
  })`;

const exists = (where: string) =>
  `SELECT EXISTS(SELECT 1 FROM ${TABLE} ${where}) AS result;`;
const deleteAll = (where: string) => `DELETE FROM ${TABLE} ${where};`;
const updateDataset = (newDataset: string, where: string) =>
  `UPDATE ${TABLE} SET dataset = ${newDataset} ${where};`;

{
  const db = new Database(path);
  db.fileControl(constants.SQLITE_FCNTL_PERSIST_WAL, 0);

  // Merge legacy and new datasets
  {
    type Params = { $persystem: Dataset; $sum: Dataset };
    using overlapExists = db.query<{ result: boolean }, Params>(
      exists(whereDatasetsOverlap("$persystem", "$sum")),
    );
    using nonOverlapingExists = db.query<{ result: boolean }, Params>(
      exists(whereDatasetsDontOverlap("$persystem", "$sum")),
    );
    using deleteOverlaping = db.query<void, Params>(
      deleteAll(whereDatasetsOverlap("$persystem", "$sum")),
    );
    using updateNonOverlaping = db.query<void, Params>(
      updateDataset("$sum", whereDatasetsDontOverlap("$persystem", "$sum")),
    );

    for (const [$persystem, $sum] of correspondingMetrics) {
      console.log(`🚨 Checking datasets ${$persystem} & ${$sum}.`);

      if (overlapExists.get({ $persystem, $sum })?.result) {
        console.log(`👷 Overlap between 'default' and 'Sum' exists.`);
        console.log(`👷 Removing overlaping 'default' rows...`);

        deleteOverlaping.run({ $persystem, $sum });
        if (overlapExists.get({ $persystem, $sum })?.result) {
          console.log("❌ Failed to remove overlap. This was unexpected.");
          process.exit(1);
        } else {
          console.log("✅ Overlap successfully removed.");
        }
      } else {
        console.log("✅ There is no overlap.");
      }

      if (nonOverlapingExists.get({ $persystem, $sum })?.result) {
        console.log(`👷 Legacy 'default' variant exist.`);
        console.log(`👷 Converting to newer metric...`);

        updateNonOverlaping.run({ $persystem, $sum });
        if (nonOverlapingExists.get({ $persystem, $sum })?.result) {
          console.log(
            "❌ Failed to convert legacy variant. This was unexpected.",
          );
          process.exit(1);
        } else {
          console.log("✅ Legacy variant successfully converted.");
        }
      } else {
        console.log("✅ There is no legacy 'default' variant.");
      }
      console.log();
    }
  }

  // Day alignment fix
  {
    const datasets = db.query<{ dataset: string }, {}>(`SELECT DISTINCT dataset FROM ${TABLE}`).all({}).map(d => d.dataset);
    using delete_ = db.query(`DELETE FROM ${TABLE} WHERE dataset = $dataset AND variant = $variant`);
    using insert = db.query(`INSERT INTO ${TABLE} (dataset, variant, x, y) VALUES ($dataset, $variant, $x, $y)`);

    console.log(`🚨 Detected ${datasets.length} distinct datasets.`);

    for (const dataset of datasets) {
      const variants = db.query<{ variant: string }, { $dataset: string }>(`SELECT DISTINCT variant FROM ${TABLE} WHERE dataset = $dataset`).all({ $dataset: dataset }).map(d => d.variant);
      console.log(`👷 UTC noon aligning ${variants.length} variant${variants.length > 1 ? 's' : ''} in ${dataset}.`);

      for (const variant of variants) {
        const values = db.query<{ x: number, y: number }, { $variant: string, $dataset: string }>(`SELECT x, y FROM ${TABLE} WHERE dataset = $dataset AND variant = $variant`).all({
          $dataset: dataset,
          $variant: variant
        });

        const nextValues = new Map<number, number>();

        for (const { x, y } of values) {
          const noon = (Math.floor(x / millisInDay) + 0.5) * millisInDay;

          if (nextValues.has(noon)) {
            nextValues.set(noon, Math.max(y, nextValues.get(noon)!));
          } else {
            nextValues.set(noon, y);
          }
        }

        const call = db.transaction((values) => {
          delete_.run({ $dataset: dataset, $variant: variant });

          for (const [x, y] of values.entries())
            insert.run({ $dataset: dataset, $variant: variant, $x: x, $y: y });
        })

        call(nextValues);
      }
    }
  }
}
