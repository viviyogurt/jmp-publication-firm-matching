"""
Refined keyword lists for AI patent identification and strategic classification.

Key improvements:
1. Removed generic terms ("method", "system") that caused false positives
2. Added abbreviations (CNN, RNN, NLP, GAN, etc.)
3. More specific, context-aware keywords
4. Organized by strategic category with overlap allowed
"""

# =============================================================================
# STAGE 1: AI IDENTIFICATION KEYWORDS (INTERSECTION APPROACH)
# =============================================================================

# High-precision AI keywords (used AFTER CPC filter)
AI_KEYWORDS_INTERSECTION = [
    # Core AI (explicit)
    'artificial intelligence',
    'machine learning',
    'neural network',
    'deep learning',

    # Specific architectures
    'convolutional neural',
    'recurrent neural',
    'transformer model',
    'attention mechanism',
    'generative adversarial',
    'large language model',

    # Applications (explicitly AI)
    'computer vision',
    'natural language processing',
    'nlp',
    'reinforcement learning',
]

# AI-related CPC codes (for INTERSECTION approach)
AI_CPC_CODES = [
    'G06N',   # AI-specific computing arrangements
    'G06Q',   # Data processing for business (AI applications)
    'G10L',   # Speech recognition/analysis
    'H04N',   # Image processing/communication (computer vision)
]

# =============================================================================
# STAGE 2: STRATEGIC CLASSIFICATION KEYWORDS (REFINED)
# =============================================================================

# Infrastructure/Hardware Keywords
INFRASTRUCTURE_KEYWORDS = [
    # Hardware accelerators (AI-specific)
    'gpu',
    'tpu',
    'tensor processing unit',
    'fpga',
    'asic',
    'neural processing unit',
    'ai accelerator',
    'ai chip',
    'hardware accelerator',

    # Computing systems (AI-specific)
    'cloud ai',
    'edge ai',
    'distributed machine learning',
    'parallel computing for neural',
    'computing architecture for neural',

    # Memory (AI-specific)
    'high bandwidth memory',
    'hbm',
    'memory for neural network',
    'memory for deep learning',
]

# Algorithm/Model Keywords
ALGORITHM_KEYWORDS = [
    # Neural architectures (with abbreviations)
    'convolutional neural network',
    'cnn',
    'recurrent neural network',
    'rnn',
    'lstm',
    'long short-term memory',
    'gru',
    'gated recurrent unit',

    # Transformers and attention
    'transformer model',
    'transformer architecture',
    'attention mechanism',
    'self-attention',

    # Generative models
    'generative adversarial network',
    'gan',
    'variational autoencoder',
    'vae',
    'diffusion model',

    # Training methods (AI-specific)
    'backpropagation',
    'back propagation',
    'gradient descent',
    'reinforcement learning',
    'deep learning',
    'transfer learning',
    'federated learning',

    # NLP (with abbreviations)
    'natural language processing',
    'nlp',
    'large language model',
    'llm',
    'bert',
    'gpt',

    # Computer vision algorithms
    'computer vision algorithm',
    'image recognition algorithm',
    'object detection algorithm',
]

# Application/Software Keywords
APPLICATION_KEYWORDS = [
    # Autonomous systems
    'autonomous driving',
    'self-driving',
    'self driving',
    'autonomous vehicle',

    # Healthcare AI
    'ai medical diagnosis',
    'medical imaging ai',
    'ai diagnostic',

    # Business AI
    'ai recommendation',
    'recommendation system',
    'fraud detection',
    'predictive analytics',

    # Assistants
    'chatbot',
    'virtual assistant',
    'conversational ai',

    # Recognition applications (not algorithms)
    'speech recognition application',
    'speech recognition system',
    'face recognition system',
    'image recognition system',
]

# =============================================================================
# CONTEXT-AWARE KEYWORDS (Handle ambiguous terms)
# =============================================================================

# Terms that require context to avoid false positives
CONTEXTUAL_KEYWORDS = {
    'dropout': {
        'valid_context': ['neural', 'network', 'training', 'deep learning', 'machine learning'],
        'invalid_context': ['voltage', 'regulator', 'electronics', 'power'],
        'category': 'algorithm',
    },
    'memory': {
        'valid_context': ['neural', 'network', 'deep learning', 'model'],
        'invalid_context': ['ram', 'rom', 'flash', 'storage'],
        'category': 'infrastructure',
    },
}

# =============================================================================
# ABBREVIATION EXPANSIONS (For matching)
# =============================================================================

ABBREVIATIONS = {
    'CNN': ['convolutional neural network', 'cnn'],
    'RNN': ['recurrent neural network', 'rnn'],
    'LSTM': ['long short-term memory', 'lstm'],
    'GRU': ['gated recurrent unit', 'gru'],
    'NLP': ['natural language processing', 'nlp'],
    'BERT': ['bidirectional encoder representations', 'bert'],
    'GPT': ['generative pre-trained transformer', 'gpt'],
    'LLM': ['large language model', 'llm'],
    'GAN': ['generative adversarial network', 'gan'],
    'VAE': ['variational autoencoder', 'vae'],
    'TPU': ['tensor processing unit', 'tpu'],
    'GPU': ['graphics processing unit', 'gpu'],
    'FPGA': ['field-programmable gate array', 'fpga'],
    'ASIC': ['application-specific integrated circuit', 'asic'],
    'HBM': ['high bandwidth memory', 'hbm'],
}

# =============================================================================
# NEGATIVE KEYWORDS (Exclude these patterns)
# =============================================================================

# Generic terms that should NOT trigger classification alone
NEGATIVE_KEYWORDS = [
    'method and apparatus for',  # Legal boilerplate
    'system comprising',         # Too generic
    'associated with',           # Too generic
    'installation assembly',     # Not AI
]

# =============================================================================
# POSITIVE CONTROLS (Known AI patents for validation)
# =============================================================================

# High-confidence AI companies (for validation)
POSITIVE_CONTROL_COMPANIES = [
    'Google',
    'DeepMind',
    'OpenAI',
    'NVIDIA',
    'Microsoft',
    'Amazon',
    'Meta',
    'Facebook',
]

# Expected AI patent rates for these companies (>80%)
EXPECTED_AI_RATES = {company: 0.80 for company in POSITIVE_CONTROL_COMPANIES}
