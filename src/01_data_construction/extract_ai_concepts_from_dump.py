#!/usr/bin/env python3
"""
Extract AI-related concepts with high precision using comprehensive ACM CCS taxonomy.

Strategy:
1. Use comprehensive AI concept list (ACM CCS + modern additions)
2. Use known AI concept IDs (most reliable)
3. Fuzzy match concept names against comprehensive list
4. Apply exclusion rules to avoid false positives
5. Optionally use ancestors hierarchy if available
"""

import requests
import gzip
import json
from pathlib import Path
import logging
from typing import Dict, List, Set
from difflib import SequenceMatcher

# Configuration
OUTPUT_DIR = Path("/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_flattened_batches")
CONCEPTS_FILE = Path("/home/kurtluo/yannan/jmp/data/raw/patents/part_000.gz")
AI_CONCEPTS_FILE = OUTPUT_DIR / "_ai_concept_ids_complete.json"
AI_CONCEPT_IDS_FILE = OUTPUT_DIR / "_ai_concept_ids_list.json"

# AI root concept ID
AI_ROOT_ID = "https://openalex.org/C154945302"  # Artificial intelligence
CS_ROOT_ID = "https://openalex.org/C41008148"    # Computer Science

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_comprehensive_ai_concepts_list() -> Set[str]:
    """Return comprehensive list of AI concepts from ACM CCS taxonomy + modern additions."""
    
    # Layer 1: Root Concepts
    layer1 = [
        "artificial intelligence",
        "machine learning",
        "computer science"
    ]
    
    # Layer 2: ACM CCS 2012 I.2 Main Branches
    layer2 = [
        # I.2.0 General
        "artificial intelligence theory",
        "philosophical foundations of ai",
        "cognitive science",
        "computational intelligence",
        
        # I.2.1 Applications
        "expert system",
        "knowledge-based system",
        "rule-based system",
        "business intelligence",
        "intelligent agent",
        "software agent",
        
        # I.2.2 Automatic Programming
        "program synthesis",
        "inductive programming",
        "genetic programming",
        "automatic code generation",
        
        # I.2.3 Deduction and Theorem Proving
        "automated theorem proving",
        "logic programming",
        "prolog",
        "first-order logic",
        "description logic",
        "temporal logic",
        "modal logic",
        "nonmonotonic reasoning",
        "default reasoning",
        "abductive reasoning",
        
        # I.2.4 Knowledge Representation
        "knowledge representation",
        "ontology",
        "semantic web",
        "knowledge graph",
        "frame system",
        "semantic network",
        "conceptual graph",
        "belief revision",
        
        # I.2.5 Programming Languages
        "constraint programming",
        "logic programming language",
        "functional programming for ai",
        
        # I.2.6 LEARNING (CORE ML)
        "machine learning theory",
        "statistical learning theory",
        "computational learning theory",
        "supervised learning",
        "unsupervised learning",
        "semi-supervised learning",
        "reinforcement learning",
        "online learning",
        "active learning",
        "transfer learning",
        "multi-task learning",
        "meta learning",
        "few-shot learning",
        "zero-shot learning",
        "one-shot learning",
        "continual learning",
        "lifelong learning",
        "incremental learning",
        "self-supervised learning",
        "contrastive learning",
        "causal learning",
        "federated learning",
        "distributed machine learning",
        "multi-instance learning",
        "multi-label learning",
        "imbalanced learning",
        "adversarial learning",
        
        # I.2.7 Natural Language Processing
        "natural language processing",
        "computational linguistics",
        "text mining",
        "information extraction",
        "named entity recognition",
        "relation extraction",
        "event extraction",
        "machine translation",
        "neural machine translation",
        "statistical machine translation",
        "natural language generation",
        "text generation",
        "dialogue system",
        "conversational ai",
        "chatbot",
        "question answering",
        "reading comprehension",
        "text summarization",
        "abstractive summarization",
        "extractive summarization",
        "sentiment analysis",
        "opinion mining",
        "text classification",
        "language modeling",
        "masked language modeling",
        "speech recognition",
        "automatic speech recognition",
        "speech synthesis",
        "text-to-speech",
        "speaker recognition",
        "speech emotion recognition",
        "discourse analysis",
        "pragmatics",
        "coreference resolution",
        "semantic role labeling",
        "dependency parsing",
        "constituency parsing",
        "morphological analysis",
        "phonology",
        
        # I.2.8 Problem Solving, Search, Control
        "search algorithm",
        "heuristic search",
        "a* search",
        "beam search",
        "monte carlo tree search",
        "constraint satisfaction",
        "constraint optimization",
        "planning",
        "automated planning",
        "hierarchical task network",
        "markov decision process",
        "partially observable markov decision process",
        "dynamic programming",
        "reinforcement learning planning",
        "game playing",
        "minimax",
        "alpha-beta pruning",
        "nash equilibrium",
        "game theory",
        
        # I.2.9 Robotics
        "robotics",
        "robot learning",
        "reinforcement learning for robotics",
        "imitation learning",
        "inverse reinforcement learning",
        "sim2real",
        "embodied ai",
        "embodied intelligence",
        "visual navigation",
        "affordance learning",
        "manipulation planning",
        "motion planning",
        "simultaneous localization and mapping",
        "slam",
        "robot perception",
        "human-robot interaction",
        
        # I.2.10 Vision and Scene Understanding
        "computer vision",
        "image processing",
        "image understanding",
        "object detection",
        "object recognition",
        "image classification",
        "semantic segmentation",
        "instance segmentation",
        "panoptic segmentation",
        "pose estimation",
        "human pose estimation",
        "action recognition",
        "video analysis",
        "video understanding",
        "video generation",
        "optical flow",
        "stereo vision",
        "3d reconstruction",
        "structure from motion",
        "visual slam",
        "visual odometry",
        "scene understanding",
        "visual question answering",
        "image captioning",
        "visual reasoning",
        "visual grounding",
        
        # I.2.11 Distributed Artificial Intelligence
        "multi-agent system",
        "multi-agent reinforcement learning",
        "cooperative ai",
        "competitive ai",
        "agent-based modeling",
        "swarm intelligence",
        "collective intelligence",
        "distributed problem solving",
        "negotiation",
        "auction",
        "mechanism design",
        "emergent communication",
        "population-based training"
    ]
    
    # Layer 3: Neural Architectures & Generative Models
    layer3 = [
        # Core Architectures
        "neural network",
        "artificial neural network",
        "deep neural network",
        "feedforward neural network",
        "multilayer perceptron",
        "convolutional neural network",
        "recurrent neural network",
        "long short-term memory",
        "gated recurrent unit",
        "transformer",
        "attention mechanism",
        "self-attention",
        "multi-head attention",
        "vision transformer",
        "swin transformer",
        "graph neural network",
        "graph convolutional network",
        "graph attention network",
        "temporal graph neural network",
        "message passing neural network",
        "geometric deep learning",
        "residual neural network",
        "resnet",
        "dense neural network",
        "densenet",
        "capsule network",
        "spiking neural network",
        "neuromorphic computing",
        
        # Generative Models
        "generative model",
        "generative adversarial network",
        "gan",
        "conditional gan",
        "stylegan",
        "diffusion model",
        "denoising diffusion probabilistic model",
        "score-based generative model",
        "latent diffusion model",
        "stable diffusion",
        "variational autoencoder",
        "vae",
        "beta-vae",
        "normalizing flow",
        "autoregressive model",
        "pixelcnn",
        "pixelrnn",
        "wavelet neural network",
        "neural radiance field",
        "nerf",
        "gaussian splatting",
        "3d gaussian splatting",
        "world model",
        "latent world model",
        "energy-based model",
        
        # Self-Supervised & Representation Learning
        "representation learning",
        "feature learning",
        "embedding",
        "word embedding",
        "word2vec",
        "glove",
        "bert embedding",
        "contrastive learning",
        "simclr",
        "moco",
        "dino",
        "barlow twins",
        "vicreg",
        "masked autoencoder",
        "mae",
        "autoencoder",
        "denoising autoencoder",
        "variational autoencoder",
        "adversarial autoencoder",
        "encoder-decoder",
        "seq2seq",
        "transformer encoder",
        "transformer decoder",
        
        # Bayesian & Probabilistic Methods
        "bayesian machine learning",
        "bayesian deep learning",
        "variational inference",
        "markov chain monte carlo",
        "mcmc",
        "gaussian process",
        "probabilistic programming",
        "bayesian optimization",
        "uncertainty quantification",
        "model calibration",
        "conformal prediction",
        "evidence lower bound",
        "elbo"
    ]
    
    # Layer 4: Algorithms, Techniques & Infrastructure
    layer4 = [
        # Classical ML Algorithms
        "support vector machine",
        "svm",
        "kernel method",
        "radial basis function kernel",
        "decision tree",
        "random forest",
        "gradient boosting",
        "xgboost",
        "lightgbm",
        "catboost",
        "adaboost",
        "ensemble learning",
        "bagging",
        "boosting",
        "stacking",
        "voting classifier",
        "k-nearest neighbors",
        "knn",
        "naive bayes",
        "logistic regression",
        "linear regression",
        "ridge regression",
        "lasso regression",
        "elastic net",
        "principal component analysis",
        "pca",
        "t-sne",
        "umap",
        "isomap",
        "k-means clustering",
        "hierarchical clustering",
        "dbscan",
        "spectral clustering",
        "gaussian mixture model",
        "expectation maximization",
        "em algorithm",
        
        # Optimization & Training
        "stochastic gradient descent",
        "sgd",
        "adam optimizer",
        "rmsprop",
        "adagrad",
        "learning rate scheduling",
        "learning rate decay",
        "warmup",
        "gradient clipping",
        "gradient accumulation",
        "mixed precision training",
        "fp16 training",
        "bf16 training",
        "distributed training",
        "data parallelism",
        "model parallelism",
        "pipeline parallelism",
        "tensor parallelism",
        "zero redundancy optimizer",
        "zero",
        "deepspeed",
        "megatron",
        
        # Model Efficiency & Deployment
        "model compression",
        "pruning",
        "structured pruning",
        "unstructured pruning",
        "quantization",
        "post-training quantization",
        "quantization-aware training",
        "knowledge distillation",
        "teacher-student learning",
        "neural architecture search",
        "nas",
        "efficient neural network",
        "mobile neural network",
        "efficientnet",
        "mobilenet",
        "shufflenet",
        "edge ai",
        "tinyml",
        "on-device machine learning",
        "model serving",
        "inference optimization",
        "tensorrt",
        "onnx runtime",
        "mlops",
        "feature store",
        "model registry",
        "model monitoring",
        "drift detection",
        
        # Evaluation & Benchmarks
        "model evaluation",
        "cross-validation",
        "k-fold cross-validation",
        "leave-one-out cross-validation",
        "glue benchmark",
        "super glue",
        "mmlu",
        "helm",
        "big bench",
        "leaderboard",
        "fairness benchmark",
        "robustness benchmark",
        "adversarial robustness benchmark",
        "out-of-distribution detection",
        "anomaly detection",
        "novelty detection",
        
        # AI Safety & Alignment
        "ai safety",
        "ai alignment",
        "ai ethics",
        "explainable ai",
        "xai",
        "interpretable machine learning",
        "model interpretability",
        "feature attribution",
        "shapley value",
        "lime",
        "counterfactual explanation",
        "algorithmic fairness",
        "bias mitigation",
        "fairness in machine learning",
        "demographic parity",
        "equalized odds",
        "adversarial robustness",
        "adversarial example",
        "adversarial attack",
        "adversarial training",
        "certified robustness",
        "privacy-preserving machine learning",
        "differential privacy",
        "federated learning",
        "secure multi-party computation",
        "homomorphic encryption",
        "reinforcement learning from human feedback",
        "rlhf",
        "constitutional ai",
        "scalable oversight",
        "debate",
        "iterated amplification",
        "recursive reward modeling",
        "red teaming",
        "ai red teaming",
        "model jailbreak",
        "prompt injection",
        
        # Causal Inference
        "causal inference",
        "causal discovery",
        "structural causal model",
        "do-calculus",
        "counterfactual reasoning",
        "instrumental variable",
        "propensity score matching",
        "causal representation learning",
        "causal reinforcement learning",
        "granger causality",
        
        # Neurosymbolic & Hybrid AI
        "neurosymbolic ai",
        "neural-symbolic integration",
        "differentiable reasoning",
        "logic tensor network",
        "neural theorem prover",
        "program induction",
        "neural program synthesis",
        
        # Multimodal & Foundation Models
        "foundation model",
        "large language model",
        "llm",
        "gpt",
        "bert",
        "t5",
        "llama",
        "mistral",
        "claude",
        "gemini",
        "multimodal learning",
        "vision-language model",
        "clip",
        "flava",
        "blip",
        "retrieval augmented generation",
        "rag",
        "prompt engineering",
        "in-context learning",
        "chain-of-thought prompting",
        "tree-of-thoughts",
        "self-consistency",
        "program-aided language model",
        "pal",
        
        # AI Hardware
        "ai accelerator",
        "gpu computing",
        "tensor processing unit",
        "tpu",
        "neural processing unit",
        "npu",
        "field programmable gate array",
        "fpga for ai",
        "application specific integrated circuit",
        "asic for ai",
        "optical computing for ai",
        "quantum machine learning",
        "quantum neural network"
    ]
    
    # Layer 5: Cross-disciplinary Applications
    layer5 = [
        # AI for Science
        "ai for science",
        "scientific machine learning",
        "physics-informed neural network",
        "neural operator",
        "fourier neural operator",
        "computational biology",
        "bioinformatics",
        "protein folding",
        "alphafold",
        "roseTTAFold",
        "drug discovery",
        "molecular dynamics",
        "climate modeling",
        "earth system modeling",
        "computational chemistry",
        "materials discovery",
        
        # Healthcare & Medicine
        "medical ai",
        "medical imaging",
        "radiology ai",
        "digital pathology",
        "clinical decision support",
        "electronic health record mining",
        "genomic medicine",
        
        # Other Domains
        "ai for education",
        "intelligent tutoring system",
        "ai for finance",
        "algorithmic trading",
        "ai for agriculture",
        "precision agriculture",
        "ai for transportation",
        "autonomous driving",
        "ai for energy",
        "smart grid"
    ]
    
    # Combine all layers
    all_concepts = layer1 + layer2 + layer3 + layer4 + layer5
    
    # Create normalized set (lowercase, for matching)
    normalized_set = set()
    for concept in all_concepts:
        normalized_set.add(concept.lower().strip())
    
    return normalized_set


def get_known_ai_concept_ids() -> Set[str]:
    """Return verified AI concept IDs from OpenAlex."""
    return {
        "https://openalex.org/C154945302",  # Artificial intelligence
        "https://openalex.org/C119857082",  # Machine learning
        "https://openalex.org/C108583219",  # Deep learning
        "https://openalex.org/C50644808",   # Artificial neural network
        "https://openalex.org/C31972630",   # Computer vision
        "https://openalex.org/C204321447",  # Natural language processing
        "https://openalex.org/C81363708",   # Convolutional neural network
        "https://openalex.org/C147168706",  # Recurrent neural network
        "https://openalex.org/C2984842247", # Deep neural networks
        "https://openalex.org/C157170001",  # Applications of artificial intelligence
        "https://openalex.org/C176777502",  # Anticipation (artificial intelligence)
        "https://openalex.org/C77967617",   # Active learning (machine learning)
        "https://openalex.org/C46686674",   # Boosting (machine learning)
        "https://openalex.org/C162027153",  # Artificial general intelligence
        "https://openalex.org/C30112582",   # Artificial Intelligence System
        "https://openalex.org/C207453521",  # Artificial intelligence, situated approach
        "https://openalex.org/C26205005",   # Symbolic artificial intelligence
        "https://openalex.org/C41008148",   # Computer Science (parent)
    }


def get_exclusion_keywords() -> List[str]:
    """Return keywords that indicate non-AI concepts."""
    return [
        # Biology/Neuroscience (not AI)
        'neural stem',
        'neural development',
        'neural cell',
        'neural activity',
        'neural system',
        'neural correlates',
        'biological neural',
        'ciliary',
        'ganglion',
        'neural crest',
        'neural tube',
        'neural plasticity',
        'synaptic',
        
        # Chemistry (not AI)
        'organic chemistry',
        'inorganic chemistry',
        'organic radical',
        'organic matter',
        'organic compound',
        'inorganic compound',
        'ligand (biochemistry)',
        'radical battery',
        
        # Psychology (not AI)
        'pattern recognition (psychology)',
        'psychology',
        'cognitive psychology',
        
        # Economics/Business (not AI)
        'industrial organization',
        'margin (economics)',
        'organizational behavior',
        
        # Other non-AI
        'microorganism',
        'organism',
        'soil classification',
        'battery',
        'radical',
        'organic radical battery',
    ]


def fuzzy_match_concept_name(concept_name: str, ai_concepts_set: Set[str], threshold: float = 0.85) -> bool:
    """Check if concept name matches any AI concept with fuzzy matching."""
    concept_lower = concept_name.lower().strip()
    
    # Exact match first
    if concept_lower in ai_concepts_set:
        return True
    
    # Check if any AI concept is a substring
    for ai_concept in ai_concepts_set:
        if ai_concept in concept_lower or concept_lower in ai_concept:
            # Additional validation for partial matches
            if len(ai_concept) > 5:  # Avoid matching very short terms
                similarity = SequenceMatcher(None, concept_lower, ai_concept).ratio()
                if similarity >= threshold:
                    return True
    
    # Check for key phrases
    key_phrases = [
        '(machine learning)', '(artificial intelligence)', '(ai)',
        'machine learning', 'artificial intelligence', 'deep learning',
        'neural network', 'computer vision', 'natural language processing'
    ]
    
    for phrase in key_phrases:
        if phrase in concept_lower:
            return True
    
    return False


def check_concepts_file() -> Path:
    """Check if the local concepts file exists."""
    logger.info("Step 1: Checking for local concepts file...")
    
    if not CONCEPTS_FILE.exists():
        raise FileNotFoundError(
            f"Concepts file not found at {CONCEPTS_FILE}\n"
            f"Please ensure the file exists at the specified path."
        )
    
    file_size = CONCEPTS_FILE.stat().st_size
    logger.info(f"Found concepts file: {CONCEPTS_FILE}")
    logger.info(f"File size: {file_size:,} bytes ({file_size / 1024 / 1024:.1f} MB)")
    
    return CONCEPTS_FILE


def parse_concepts_dump(concepts_file: Path) -> List[Dict]:
    """Parse the concepts JSON Lines dump."""
    logger.info("Step 2: Parsing concepts dump...")
    
    concepts = []
    
    with gzip.open(concepts_file, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                if not line.strip():
                    continue
                
                concept_json = json.loads(line.strip())
                
                concept = {
                    "id": concept_json.get("id", ""),
                    "display_name": concept_json.get("display_name", ""),
                    "level": concept_json.get("level", 0),
                    "ancestors": concept_json.get("ancestors"),
                    "wikidata": concept_json.get("wikidata", ""),
                    "works_count": concept_json.get("works_count", 0),
                    "description": concept_json.get("description", "")
                }
                concepts.append(concept)
                
                if len(concepts) % 10000 == 0:
                    logger.info(f"    Parsed {len(concepts):,} concepts...")
            
            except json.JSONDecodeError as e:
                logger.warning(f"    Error parsing line {line_num}: {e}")
                continue
            except Exception as e:
                logger.warning(f"    Error processing line {line_num}: {e}")
                continue
    
    logger.info(f"Parsed {len(concepts):,} total concepts")
    return concepts


def build_ai_concept_list(concepts: List[Dict]) -> List[Dict]:
    """Build high-precision AI concept list using comprehensive taxonomy."""
    logger.info("Step 3: Building high-precision AI concept list...")
    
    # Load comprehensive AI concepts list
    comprehensive_ai_concepts = get_comprehensive_ai_concepts_list()
    known_ai_ids = get_known_ai_concept_ids()
    exclusion_keywords = get_exclusion_keywords()
    
    logger.info(f"Loaded {len(comprehensive_ai_concepts)} AI concepts from comprehensive taxonomy")
    logger.info(f"Loaded {len(known_ai_ids)} known AI concept IDs")
    
    ai_concepts = []
    ai_concept_ids = set()
    
    # Method 1: Known AI concept IDs (most reliable)
    logger.info("Method 1: Finding known AI concept IDs...")
    for concept in concepts:
        if concept["id"] in known_ai_ids:
            ai_concepts.append(concept)
            ai_concept_ids.add(concept["id"])
    
    logger.info(f"  Found {len(ai_concepts)} concepts by known IDs")
    
    # Method 2: Match against comprehensive taxonomy
    logger.info("Method 2: Matching against comprehensive AI taxonomy...")
    taxonomy_matches = 0
    for concept in concepts:
        if concept["id"] in ai_concept_ids:
            continue
        
        name = concept.get("display_name", "")
        name_lower = name.lower()
        
        # Check exclusions first
        if any(exclude in name_lower for exclude in exclusion_keywords):
            continue
        
        # Fuzzy match against comprehensive list
        if fuzzy_match_concept_name(name, comprehensive_ai_concepts):
            ai_concepts.append(concept)
            ai_concept_ids.add(concept["id"])
            taxonomy_matches += 1
    
    logger.info(f"  Found {taxonomy_matches} additional concepts via taxonomy matching")
    
    # Method 3: Use ancestors if available
    concepts_with_ancestors = [c for c in concepts if c.get("ancestors")]
    if concepts_with_ancestors:
        logger.info(f"Method 3: Checking {len(concepts_with_ancestors)} concepts with ancestors...")
        ancestor_matches = 0
        for concept in concepts_with_ancestors:
            if concept["id"] in ai_concept_ids:
                continue
            
            ancestors = concept.get("ancestors", [])
            ancestor_ids = []
            for anc in ancestors:
                if isinstance(anc, dict):
                    ancestor_ids.append(anc.get("id"))
                elif isinstance(anc, str):
                    ancestor_ids.append(anc)
            
            # Check if any ancestor is an AI concept
            if any(aid in ai_concept_ids for aid in ancestor_ids):
                # Additional check: exclude if it matches exclusion keywords
                name_lower = concept.get("display_name", "").lower()
                if not any(exclude in name_lower for exclude in exclusion_keywords):
                    ai_concepts.append(concept)
                    ai_concept_ids.add(concept["id"])
                    ancestor_matches += 1
        
        logger.info(f"  Found {ancestor_matches} additional concepts via ancestors")
    
    # Sort by works_count
    ai_concepts.sort(key=lambda x: x.get("works_count", 0), reverse=True)
    
    logger.info(f"Final AI concepts count: {len(ai_concepts)}")
    
    return ai_concepts


def extract_concept_ids(ai_concepts: List[Dict]) -> Dict[str, List[str]]:
    """Extract concept IDs in different formats."""
    logger.info("Step 4: Extracting concept IDs in multiple formats...")
    
    ids_full_url = []
    ids_with_c = []
    ids_numeric = []
    concept_details = {}
    
    for concept in ai_concepts:
        full_id = concept["id"]
        concept_id = full_id.split("/")[-1]
        numeric_id = concept_id.replace("C", "")
        
        ids_full_url.append(full_id)
        ids_with_c.append(concept_id)
        ids_numeric.append(numeric_id)
        
        concept_details[concept_id] = {
            "id": concept_id,
            "full_id": full_id,
            "numeric_id": numeric_id,
            "display_name": concept["display_name"],
            "level": concept["level"],
            "works_count": concept["works_count"],
            "wikidata": concept.get("wikidata", ""),
            "description": concept.get("description", "")
        }
    
    return {
        "full_urls": ids_full_url,
        "with_c_prefix": ids_with_c,
        "numeric_only": ids_numeric,
        "all_formats": list(set(ids_full_url + ids_with_c + ids_numeric)),
        "concept_details": concept_details
    }


def evaluate_precision_recall(ai_concepts: List[Dict]) -> Dict:
    """Evaluate precision and recall by checking for false positives."""
    logger.info("Step 5: Evaluating precision and recall...")
    
    exclusion_keywords = get_exclusion_keywords()
    false_positives = []
    true_positives = []
    
    for concept in ai_concepts:
        name_lower = concept.get("display_name", "").lower()
        
        # Check for obvious false positives
        is_false_positive = any(exclude in name_lower for exclude in exclusion_keywords)
        
        if is_false_positive:
            false_positives.append(concept)
        else:
            true_positives.append(concept)
    
    total = len(ai_concepts)
    fp_count = len(false_positives)
    tp_count = len(true_positives)
    
    # Precision = TP / (TP + FP)
    precision = tp_count / total if total > 0 else 0.0
    
    # Recall is harder to calculate without ground truth
    # We can estimate based on known AI concepts found
    known_ai_ids = get_known_ai_concept_ids()
    known_found = sum(1 for c in ai_concepts if c["id"] in known_ai_ids)
    known_total = len(known_ai_ids)
    recall_on_known = known_found / known_total if known_total > 0 else 0.0
    
    logger.info(f"Precision evaluation:")
    logger.info(f"  Total concepts: {total}")
    logger.info(f"  True positives: {tp_count}")
    logger.info(f"  False positives: {fp_count}")
    logger.info(f"  Precision: {precision:.2%}")
    logger.info(f"  Recall (on known concepts): {recall_on_known:.2%} ({known_found}/{known_total})")
    
    if false_positives:
        logger.warning(f"\nFalse positives found ({len(false_positives)}):")
        for fp in false_positives[:10]:  # Show first 10
            logger.warning(f"  - {fp['display_name']} ({fp['id']})")
    
    return {
        "total": total,
        "true_positives": tp_count,
        "false_positives": fp_count,
        "precision": precision,
        "recall_on_known": recall_on_known,
        "known_found": known_found,
        "known_total": known_total,
        "false_positive_list": false_positives[:20]  # First 20 for review
    }


def main():
    """Main function."""
    logger.info("=" * 70)
    logger.info("EXTRACTING AI CONCEPTS (COMPREHENSIVE TAXONOMY)")
    logger.info("=" * 70)
    
    # Step 1: Check file
    concepts_file = check_concepts_file()
    
    # Step 2: Parse concepts
    concepts = parse_concepts_dump(concepts_file)
    
    # Step 3: Build AI concept list
    ai_concepts = build_ai_concept_list(concepts)
    
    # Step 4: Extract concept IDs
    ai_concept_data = extract_concept_ids(ai_concepts)
    
    # Step 5: Evaluate precision/recall
    evaluation = evaluate_precision_recall(ai_concepts)
    
    # Step 6: Save results
    logger.info("Step 6: Saving results...")
    
    output_data = {
        "source_file": str(concepts_file),
        "ai_root_concept": AI_ROOT_ID,
        "total_ai_concepts": len(ai_concepts),
        "extraction_method": "comprehensive_taxonomy_with_exclusions",
        "taxonomy_size": len(get_comprehensive_ai_concepts_list()),
        "evaluation": evaluation,
        "concept_ids": ai_concept_data,
        "concepts": ai_concepts
    }
    
    with open(AI_CONCEPTS_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Save ID list
    id_list_data = {
        "source_file": str(concepts_file),
        "ai_root_concept": AI_ROOT_ID,
        "total_concepts": len(ai_concepts),
        "extraction_method": "comprehensive_taxonomy_with_exclusions",
        "taxonomy_size": len(get_comprehensive_ai_concepts_list()),
        "evaluation": evaluation,
        "all_concept_ids": ai_concept_data["all_formats"],
        "concept_ids_with_c": ai_concept_data["with_c_prefix"],
        "concept_ids_numeric": ai_concept_data["numeric_only"],
        "concept_details": ai_concept_data["concept_details"]
    }
    
    with open(AI_CONCEPT_IDS_FILE, 'w') as f:
        json.dump(id_list_data, f, indent=2)
    
    logger.info(f"Saved to: {AI_CONCEPT_IDS_FILE}")
    
    # Print summary
    logger.info("=" * 70)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total AI concepts found: {len(ai_concepts):,}")
    logger.info(f"Precision: {evaluation['precision']:.2%}")
    logger.info(f"Recall (on known concepts): {evaluation['recall_on_known']:.2%}")
    logger.info(f"\nTop 30 AI concepts:")
    for i, concept in enumerate(ai_concepts[:30], 1):
        logger.info(f"  {i:2d}. {concept['display_name']:50s} | {concept['works_count']:>12,} works | Level {concept['level']}")


if __name__ == "__main__":
    main()
