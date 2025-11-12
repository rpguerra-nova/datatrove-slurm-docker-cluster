import argparse
import os

from datatrove.executor import SlurmPipelineExecutor, LocalPipelineExecutor
from datatrove.pipeline.readers import WarcReader, JsonlReader
from datatrove.pipeline.extractors import Trafilatura, TrafilaturaMetadata
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.post_scraping import PostScraper
from datatrove.pipeline.filters import LanguageFilter, URLFilter
from datatrove.utils.typeshelper import Languages
from datatrove.utils.hashing import HashConfig
from datatrove.pipeline.filters import (
    C4QualityFilter,
    C4BadWordsFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    LatestURLFilter
)
from datatrove.pipeline.dedup import MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import (
    MinhashConfig,
    MinhashDedupBuckets,
    MinhashDedupCluster,
    MinhashDedupFilter,
)
from datatrove.pipeline.formatters import PIIFormatter, FTFYFormatter, SymbolLinesFormatter
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.base import PipelineStep
from datatrove.data import DocumentsPipeline
import time

class EnhancedWarcReader(WarcReader):
    """ Custom WarcReader that logs errors and skips files that raise exceptions instead of stopping the task """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import logging
        self.logger = logging.getLogger(__name__)

    def read_file(self, filepath: str):
        try:
            yield from super().read_file(filepath)
        except Exception as e:
            self.logger.error(f"%%%%% Error processing file {filepath}: {e} %%%%%")

# Rehydrater class to rehydrate documents based on their minhash cluster size
class Rehydrater(PipelineStep):
    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        import bisect
        upsampling_weights = {1: 1, 2: 2, 3: 3, 5: 5, 100: 8, 1000: 1}
        # Sorted keys
        limits = sorted(upsampling_weights.keys())

        for doc in data:
            upsampling_weight = upsampling_weights[
                limits[bisect.bisect_right(limits, doc.metadata["minhash_cluster_size"]) - 1]]
            doc.metadata["upsampling_weight"] = upsampling_weight
            yield doc
            # repeat each document upsampling_weight times
            # for _ in range(upsampling_weight):
            #     yield doc

def get_stop_words_fineweb2():
    # List of portuguese stop words from FineWeb2 dataset
    return [
        "de", "a", "e", "o", "em", "do", "da", "que", "um", "no", "uma",
        "com", "para", "na", "é", "foi"
    ]

def run_full_pipeline(files_path, output_path, logs_path, num_tasks=1, num_workers=1):
    input_reader = JsonlReader(
        data_folder=f"{output_path}/filtering_output",
        glob_pattern="*.jsonl.gz"
    )

    filtering_output_path = f"{logs_path}/filtered"
    dedup_output_path = f"{logs_path}/dedup"
    dedup_logs_path = f"{logs_path}/dedup_logs"
    dedup_tasks = num_tasks//5

    # Configure Minhash
    minhash_config = MinhashConfig(
        hash_config=HashConfig(
            hash_fc="xxhash",
            precision=64,  # better precision -> fewer false positives (collisions)
        ),
        num_buckets=14,
        hashes_per_bucket=8,
        n_grams=5,
    )

    collection_job_id = os.path.basename(files_path).lower()

    # WARC scraping and filtering stage
    scrape_filter_stage = SlurmPipelineExecutor(
        pipeline=[
            EnhancedWarcReader(
                data_folder=files_path,
                glob_pattern="**/*arc.gz",
                recursive=True,
            ),
            URLFilter(exclusion_writer=JsonlWriter(f"{filtering_output_path}/removed/0_url_filter")),
            LambdaFilter(lambda doc: (doc_url := doc.metadata.get('url')) is not None and
                                     ('/pt-br' not in doc_url and '/pt_br/' not in doc_url
                                      and '.com/br' not in doc_url and '.br' not in doc_url and '/br/' not in doc_url
                                      )
                         ),
            TrafilaturaMetadata(favour_precision=True,
                                timeout=30,  # Increase timeout (in seconds)
                                ),
            PostScraper(
                exclusion_writer=JsonlWriter(f"{filtering_output_path}/removed/1_post_scraper"),
                remove_dup_lines=True,
                remove_short_lines=True,
            ),
            LanguageFilter(
                languages=[Languages.portuguese__latn],
                language_threshold=0.799,
                backend="glotlid",
                exclusion_writer=JsonlWriter(
                    f"{filtering_output_path}/removed/2_lang_exclusion",
                    output_filename="${language}/${rank}.jsonl.gz"
                )
            ),
            GopherRepetitionFilter(
                exclusion_writer=JsonlWriter(f"{filtering_output_path}/removed/3_gopher_rep"),
                language=Languages.portuguese__latn,
                dup_para_frac=None,
                dup_line_char_frac=None,
                dup_para_char_frac=None,
                dup_line_frac=0.287,
                top_n_grams=((2, 0.371), (3, 0.191), (4, 0.163)),
                dup_n_grams=((5, 0.163), (6, 0.153), (7, 0.141), (8, 0.13),
                             (9, 0.119), (10, 0.108))
            ),
            FineWebQualityFilter(
                exclusion_writer=JsonlWriter(f"{filtering_output_path}/removed/4_fineweb_qual"),
                language=Languages.portuguese__latn,
                new_line_ratio=0.186,
                line_punct_thr=0.077,
                char_duplicates_ratio=0.1,
                short_line_thr=999  # Disabled this filter
            ),
            GopherQualityFilter(
                exclusion_writer=JsonlWriter(f"{filtering_output_path}/removed/5_gopher_qual"),
                language=Languages.portuguese__latn,
                min_avg_word_length=3,
                max_avg_word_length=13,
                min_doc_words=15,
                max_non_alpha_words_ratio=0.814,
                max_ellipsis_lines_ratio=0.3,
                stop_words=get_stop_words_fineweb2(),
                min_stop_words=2
            ),
            FTFYFormatter(),  # fix encoding issues. Important in a multilingual setting
            PIIFormatter(),  # remove PII (emails, ips, etc)
            PostScraper(
                remove_pt_phone_numbers=True,
            ),
            SymbolLinesFormatter(symbols_to_remove=["|"], replace_char="\n"),  # fix trafilatura table artifacts
            TokensCounter(),
            JsonlWriter(f"{output_path}/filtering_output"),
        ],
        logging_dir=f"{logs_path}/filter_logs",
        tasks=num_tasks,  # Number of tasks to run
        workers=num_workers,  # Same as the number of cpus to use
        randomize_start_duration=180,
        job_name=f"{collection_job_id}-warc",
        cpus_per_task=2,  # Number of cpus per task
        mem_per_cpu_gb=4,
        time="0",  # No time limit
        partition="slurm_queue",
        venv_path="/opt/conda_envs/datatrove_env/bin/activate"
    )

    # Stage 1: Compute minhash signatures
    stage1 = SlurmPipelineExecutor(
        pipeline=[
            input_reader,
            MinhashDedupSignature(
                output_folder=f"{dedup_output_path}/steps/1_signatures",
                config=minhash_config,
                language=Languages.portuguese__latn
            ),
        ],
        logging_dir=f"{dedup_logs_path}/1_signatures",
        tasks=dedup_tasks,
        workers=num_workers,
        randomize_start_duration=180,
        depends=scrape_filter_stage,
        job_name=f"{collection_job_id}-mh1",
        cpus_per_task=2,
        time="0",
        slurm_logs_folder=f"{dedup_logs_path}/1_signatures/slurm_logs",
        mem_per_cpu_gb=4,
        partition="slurm_queue",
        venv_path="/opt/conda_envs/datatrove_env/bin/activate"
    )

    # Stage 2: Find matches between signatures in each bucket
    stage2 = SlurmPipelineExecutor(
        pipeline=[
            MinhashDedupBuckets(
                input_folder=f"{dedup_output_path}/steps/1_signatures",
                output_folder=f"{dedup_output_path}/steps/2_buckets",
                config=minhash_config  # MinhashConfig(hash_config=minhash_config.hash_config),
            ),
        ],
        logging_dir=f"{dedup_logs_path}/2_buckets",
        tasks=minhash_config.num_buckets,
        workers=num_workers,
        randomize_start_duration=180,
        depends=stage1,
        job_name=f"{collection_job_id}-mh2",
        cpus_per_task=2,
        time="0",
        mem_per_cpu_gb=4,
        slurm_logs_folder=f"{dedup_logs_path}/2_buckets/slurm_logs",
        partition="slurm_queue",
        venv_path="/opt/conda_envs/datatrove_env/bin/activate"
    )

    # Stage 3: Create clusters of duplicates
    stage3 = SlurmPipelineExecutor(
        pipeline=[
            MinhashDedupCluster(
                input_folder=f"{dedup_output_path}/steps/2_buckets",
                output_folder=f"{dedup_output_path}/steps/3_clusters",
                config=minhash_config,
                save_cluster_size=True,
                save_cluster_id=True
            ),
        ],
        logging_dir=f"{dedup_logs_path}/3_clusters",
        tasks=1,
        depends=stage2,
        job_name=f"{collection_job_id}-mh3",
        cpus_per_task=2,
        time="0",
        mem_per_cpu_gb=4,
        slurm_logs_folder=f"{dedup_logs_path}/3_clusters/slurm_logs",
        partition="slurm_queue",
        venv_path="/opt/conda_envs/datatrove_env/bin/activate"
    )

    # Stage 4: Remove duplicates
    stage4 = SlurmPipelineExecutor(
        pipeline=[
            input_reader,
            MinhashDedupFilter(
                input_folder=f"{dedup_output_path}/steps/3_clusters",
                exclusion_writer=JsonlWriter(f"{dedup_output_path}/removed"),
                load_cluster_ids=True,
                load_cluster_sizes=True
            ),
            Rehydrater(),
            TokensCounter(),
            JsonlWriter(output_folder=f"{output_path}/dedup_output"),
        ],
        logging_dir=f"{dedup_logs_path}/final_output",
        tasks=dedup_tasks,
        workers=num_workers,
        depends=stage3,
        job_name=f"{collection_job_id}-mh4",
        cpus_per_task=2,
        time="0",
        mem_per_cpu_gb=4,
        slurm_logs_folder=f"{dedup_logs_path}/final_output/slurm_logs",
        partition="slurm_queue",
        venv_path="/opt/conda_envs/datatrove_env/bin/activate"
    )

    # Execute the pipeline
    stage4.run()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parallel process cdxj files')
    parser.add_argument('files_path', metavar='filesPath', help='path to warcs directory')
    parser.add_argument('-o', dest='outFolderPath', help='the path where to write the processed files to',
                        required=True)
    parser.add_argument('-l', dest='logsPath', help='the path where to write the logs to',
                        required=True)
    parser.add_argument('-t', dest='numTasks', help='number of tasks to split each file amongst', default=1,
                        type=int)
    parser.add_argument('-w', dest='numWorkers', help='number of workers to split each file amongst', default=1,
                        type=int)

    args = parser.parse_args()

    if not os.path.isdir(args.files_path):
        print(f"The given warc folder doesn't exist, create it and rerun. - {args.files_path}")
        exit()

    if not os.path.isdir(args.outFolderPath):
        os.makedirs(args.outFolderPath, exist_ok=True)

    if not os.path.isdir(args.logsPath):
        os.makedirs(args.logsPath, exist_ok=True)

    run_full_pipeline(args.files_path.removesuffix("/"), args.outFolderPath.removesuffix("/"), args.logsPath.removesuffix("/"), args.numTasks, args.numWorkers)

