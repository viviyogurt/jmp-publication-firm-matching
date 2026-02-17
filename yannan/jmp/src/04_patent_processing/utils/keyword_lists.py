"""
Keyword lists for AI patent identification and strategic classification.

Two-Stage Classification Approach:
- Stage 1: AI identification (Is this AI?)
- Stage 2: Strategic classification (What type of AI?)
"""

# =============================================================================
# STAGE 1: AI IDENTIFICATION KEYWORDS
# =============================================================================

AI_KEYWORDS = [
    # Core AI terms
    'artificial intelligence',
    'machine learning',
    'neural network',
    'deep learning',

    # NLP
    'natural language processing',
    'nlp',
    'large language model',
    'language model',
    'transformer model',
    'bert',
    'gpt',
    'llm',
    'text generation',
    'sentiment analysis',

    # Computer Vision
    'computer vision',
    'image recognition',
    'object detection',
    'convolutional neural',
    'cnn',

    # Other ML techniques
    'reinforcement learning',
    'recurrent neural',
    'rnn',
    'generative adversarial',
    'gan',
    'diffusion model',
    'attention mechanism',
    'backpropagation',
    'gradient descent',

    # AI applications
    'autonomous driving',
    'speech recognition',
    'recommendation system',
    'predictive analytics',
    'pattern recognition',
]

# AI-related CPC codes (KPSS technology classes)
AI_CPC_CODES = [
    'G06N',   # AI-specific computing arrangements
    'G06Q',   # Data processing for business (AI applications)
    'G10L',   # Speech recognition/analysis
    'H04N',   # Image processing/communication (computer vision)
]

# =============================================================================
# STAGE 2: STRATEGIC CLASSIFICATION KEYWORDS
# =============================================================================

INFRASTRUCTURE_KEYWORDS = [
    # Hardware components
    'hardware',
    'chip',
    'processor',
    'gpu',
    'tpu',
    'accelerator',
    'integrated circuit',
    'semiconductor',
    'fpga',
    'asic',

    # Computing systems
    'computing system',
    'cloud platform',
    'server',
    'architecture',
    'data center',
    'parallel computing',
    'distributed computing',

    # Memory and storage
    'memory',
    'storage',
    'high bandwidth memory',
    'hbm',

    # Networking
    'network interface',
    'interconnect',
    'bandwidth',
]

ALGORITHM_KEYWORDS = [
    # Neural network architectures
    'neural network',
    'transformer',
    'convolutional',
    'recurrent',
    'attention mechanism',
    'self-attention',

    # Training methods
    'training algorithm',
    'optimization',
    'loss function',
    'backpropagation',
    'gradient descent',
    'learning rate',
    'batch normalization',

    # Model architectures
    'model architecture',
    'embedding',
    'representation learning',
    'feature extraction',
    'encoder',
    'decoder',

    # Generative models
    'generative model',
    'diffusion model',
    'generative adversarial',
    'variational autoencoder',
    'vae',

    # Other algorithmic techniques
    'reinforcement learning',
    'transfer learning',
    'federated learning',
    'meta-learning',
    'graph neural network',
    'gan',
]

APPLICATION_KEYWORDS = [
    # Business applications
    'recommendation system',
    'decision support system',
    'predictive analytics',
    'fraud detection',
    'risk assessment',

    # AI assistants and chatbots
    'chatbot',
    'virtual assistant',
    'conversational ai',
    'question answering',

    # Autonomous systems
    'autonomous driving',
    'self-driving',
    'autonomous vehicle',

    # Medical/healthcare
    'medical diagnosis',
    'diagnostic system',
    'medical imaging',

    # Recognition applications
    'image recognition',
    'speech recognition',
    'natural language understanding',
    'face recognition',

    # Other applications
    'ai application',
    'machine learning application',
    'deep learning application',
]

# =============================================================================
# CPC TO STRATEGIC CATEGORY MAPPING
# =============================================================================

CPC_TO_STRATEGIC_CATEGORY = {
    # Infrastructure/Hardware
    'G06F': 'Infrastructure',  # Electric digital data processing
    'H01L': 'Infrastructure',  # Semiconductor devices
    'H03M': 'Infrastructure',  # Coding/decoding (hardware)

    # Algorithms/Models
    'G06N': 'Algorithm',       # AI-specific computing arrangements
    'G06T': 'Algorithm',       # Image data processing (algorithms)

    # Applications
    'G06Q': 'Application',     # Business methods
    'G06K': 'Application',     # Recognition/data presentation
    'G10L': 'Application',     # Speech analysis/synthesis
    'H04N': 'Application',     # Image communication (CV applications)
}

# =============================================================================
# SOFTWARE PATENT KEYWORDS (for Alice Corp DID analysis)
# =============================================================================

SOFTWARE_KEYWORDS = [
    'software',
    'computer program',
    'algorithm',
    'data processing',
    'business method',
    'e-commerce',
    'online platform',
    'digital platform',
    'web application',
    'mobile application',
]

SOFTWARE_CPC_CODES = [
    'G06F',  # Electric digital data processing
    'G06Q',  # Business methods
]
