#!/usr/bin/env python3
"""
Model Dependency Detector for Trading Agents
=============================================

PURPOSE:
--------
This tool statically analyzes Python source code to detect which HuggingFace,
Chronos, and other ML model dependencies an agent will need at runtime. This
enables Spooky Labs to package only the required models with each agent container,
solving the GKE Autopilot 10Gi ephemeral storage limit problem for 16GB+ images.

PROBLEM CONTEXT:
----------------
Trading agents use various ML models (time-series forecasting, sentiment analysis,
etc.) loaded via HuggingFace's from_pretrained() pattern. Without knowing which
models an agent uses before deployment, we would need to include ALL possible
models in the container image, resulting in 16GB+ images that exceed GKE
Autopilot's ephemeral storage limits.

SOLUTION APPROACH:
------------------
1. Parse the agent's Python code into an Abstract Syntax Tree (AST)
2. First pass: Track all variable assignments, function return values,
   dictionary literals, list literals, and Backtrader params tuples
3. Second pass: Find all model loading calls and resolve their arguments
   to actual model name strings using the tracked data

SUPPORTED PATTERNS (26+ patterns detected):
-------------------------------------------
Basic patterns:
  - Direct string literals:    from_pretrained("bert-base-uncased")
  - Variable references:       from_pretrained(MODEL_NAME)
  - F-strings:                 from_pretrained(f"amazon/{model}")
  - String concatenation:      from_pretrained("amazon/" + model)
  - str.format():              from_pretrained("{}/{}".format(org, name))

Framework-specific patterns:
  - Backtrader params:         params = (('model_name', 'bert'),)
  - Pipeline calls:            pipeline("task", model="bert")
  - Direct instantiation:      SentenceTransformer("bert")

Advanced patterns (statically resolvable):
  - Function return tracing:   from_pretrained(get_model_name())
  - Dict literal access:       from_pretrained(config["model"])
  - List literal access:       from_pretrained(models[0])
  - Class method returns:      from_pretrained(Config.get_name())
  - Annotated assignments:     model: str = "bert"

TRULY UNRESOLVABLE (flagged for manual models.txt):
---------------------------------------------------
  - Environment variables:     os.environ.get("MODEL_NAME")
  - File reads:                open("model.txt").read()
  - Database queries:          db.get("model_name")
  - Network requests:          requests.get(...).json()["model"]

These runtime-dependent values cannot be determined via static analysis.
Users should specify them in a models.txt file.

USAGE:
------
    # Analyze a single file
    python detect_models.py agent.py

    # Analyze a directory (recursive)
    python detect_models.py /path/to/agent/

OUTPUT:
-------
JSON to stdout with:
  - "models": list of detected model names (can be downloaded/packaged)
  - "unresolved": list of calls that couldn't be resolved (need models.txt)

Summary to stderr showing counts and hints for unresolved calls.

INTEGRATION:
------------
This tool is intended to be called during the agent build pipeline:
1. User uploads agent code
2. detect_models.py analyzes the code
3. Build process downloads only the detected models
4. Container image includes only required models (~1-5GB instead of 16GB+)

AUTHOR: Spooky Labs
DATE: 2024
"""

import ast
import sys
import os
import json
from pathlib import Path
from typing import Set, Dict, List, Optional, Tuple, Union


# =============================================================================
# PATTERN DEFINITIONS
# =============================================================================
#
# These lists define which Python function/method calls we should analyze
# to extract model names. Each pattern specifies:
#   - The class or function name
#   - The method name (or "__init__" for constructors)
#   - Which argument position contains the model name (0-indexed)
#
# When we encounter a matching call in the AST, we extract and resolve the
# argument at the specified position.
# =============================================================================

LOADING_PATTERNS = [
    # -------------------------------------------------------------------------
    # HuggingFace Transformers - AutoModel Classes
    # -------------------------------------------------------------------------
    # These are the "auto" classes that automatically select the right model
    # architecture based on the model name. The model identifier is always
    # the first positional argument (position 0).
    #
    # Example: AutoModelForCausalLM.from_pretrained("gpt2")
    # -------------------------------------------------------------------------
    ("AutoModel", "from_pretrained", 0),
    ("AutoModelForCausalLM", "from_pretrained", 0),
    ("AutoModelForSeq2SeqLM", "from_pretrained", 0),
    ("AutoModelForSequenceClassification", "from_pretrained", 0),
    ("AutoModelForTokenClassification", "from_pretrained", 0),
    ("AutoModelForQuestionAnswering", "from_pretrained", 0),
    ("AutoModelForMaskedLM", "from_pretrained", 0),
    ("AutoModelForImageClassification", "from_pretrained", 0),
    ("AutoModelForObjectDetection", "from_pretrained", 0),
    ("AutoModelForAudioClassification", "from_pretrained", 0),
    ("AutoModelForSpeechSeq2Seq", "from_pretrained", 0),
    ("AutoModelForVision2Seq", "from_pretrained", 0),

    # -------------------------------------------------------------------------
    # HuggingFace Transformers - Tokenizers and Processors
    # -------------------------------------------------------------------------
    # Tokenizers/processors must match the model they're used with.
    # Detecting these ensures we download the correct tokenizer files.
    #
    # Example: AutoTokenizer.from_pretrained("bert-base-uncased")
    # -------------------------------------------------------------------------
    ("AutoTokenizer", "from_pretrained", 0),
    ("AutoProcessor", "from_pretrained", 0),
    ("AutoFeatureExtractor", "from_pretrained", 0),
    ("AutoConfig", "from_pretrained", 0),

    # -------------------------------------------------------------------------
    # HuggingFace Transformers - Specific Model Classes
    # -------------------------------------------------------------------------
    # Some users import specific model classes instead of using Auto classes.
    # We detect these to ensure coverage.
    #
    # Example: BertModel.from_pretrained("bert-base-uncased")
    # -------------------------------------------------------------------------
    ("BertModel", "from_pretrained", 0),
    ("BertTokenizer", "from_pretrained", 0),
    ("GPT2Model", "from_pretrained", 0),
    ("GPT2Tokenizer", "from_pretrained", 0),
    ("T5Model", "from_pretrained", 0),
    ("T5Tokenizer", "from_pretrained", 0),
    ("LlamaModel", "from_pretrained", 0),
    ("LlamaTokenizer", "from_pretrained", 0),

    # -------------------------------------------------------------------------
    # Amazon Chronos - Time Series Forecasting Models
    # -------------------------------------------------------------------------
    # Chronos is Amazon's time-series forecasting library, commonly used
    # in trading agents for price prediction. These models can be quite
    # large (several GB each).
    #
    # Example: ChronosPipeline.from_pretrained("amazon/chronos-t5-small")
    # -------------------------------------------------------------------------
    ("ChronosPipeline", "from_pretrained", 0),
    ("Chronos2Pipeline", "from_pretrained", 0),
    ("ChronosBoltPipeline", "from_pretrained", 0),
    ("BaseChronosPipeline", "from_pretrained", 0),

    # -------------------------------------------------------------------------
    # PEFT (Parameter-Efficient Fine-Tuning) / LoRA
    # -------------------------------------------------------------------------
    # PeftModel loads adapter weights on top of a base model.
    # IMPORTANT: The adapter name is the SECOND argument (position 1),
    # not the first! The first argument is the base model object.
    #
    # Example: PeftModel.from_pretrained(base_model, "my-lora-adapter")
    #                                    ^^^^^^^^^   ^^^^^^^^^^^^^^^^
    #                                    pos 0       pos 1 (we want this)
    # -------------------------------------------------------------------------
    ("PeftModel", "from_pretrained", 1),  # Second arg is the adapter identifier

    # -------------------------------------------------------------------------
    # Sentence Transformers
    # -------------------------------------------------------------------------
    # SentenceTransformer doesn't use from_pretrained() - it takes the model
    # name directly in __init__. We mark this with "__init__" to distinguish.
    # The INSTANTIATION_PATTERNS list below handles the actual detection.
    #
    # Example: SentenceTransformer("all-MiniLM-L6-v2")
    # -------------------------------------------------------------------------
    ("SentenceTransformer", "__init__", 0),

    # -------------------------------------------------------------------------
    # Diffusers (Image Generation)
    # -------------------------------------------------------------------------
    # While less common in trading, some agents use image generation for
    # visualization or alternative data analysis.
    #
    # Example: DiffusionPipeline.from_pretrained("stabilityai/stable-diffusion-2")
    # -------------------------------------------------------------------------
    ("DiffusionPipeline", "from_pretrained", 0),
    ("StableDiffusionPipeline", "from_pretrained", 0),
    ("AutoPipelineForText2Image", "from_pretrained", 0),
]

# =============================================================================
# PIPELINE PATTERNS
# =============================================================================
#
# HuggingFace's pipeline() function is a convenience wrapper that can load
# models. The model can be specified as:
#   - Keyword argument: pipeline("sentiment-analysis", model="bert-base")
#   - Second positional argument: pipeline("sentiment-analysis", "bert-base")
#
# We check for both patterns.
# =============================================================================

PIPELINE_PATTERNS = [
    ("pipeline", "model"),  # pipeline("task", model="...") - kwarg form
    ("pipeline", 1),        # pipeline("task", "model-name") - positional form
]

# =============================================================================
# INSTANTIATION PATTERNS
# =============================================================================
#
# Some libraries use direct class instantiation instead of from_pretrained().
# For these, the model name is passed to __init__ as the first argument.
#
# Example: SentenceTransformer("all-MiniLM-L6-v2")
#          ^^^^^^^^^^^^^^^^^^ <- class name (in this list)
#                              ^^^^^^^^^^^^^^^^^^^ <- model name (arg 0)
#
# When we see ClassName(arg) where ClassName is in this list, we extract arg.
# =============================================================================

INSTANTIATION_PATTERNS = [
    "SentenceTransformer",  # sentence-transformers library
]


# =============================================================================
# FIRST PASS: VARIABLE TRACKING
# =============================================================================
#
# Before we can resolve model names in function calls, we need to know what
# values variables hold. This class walks the AST once to collect:
#
#   1. Simple variable assignments: MODEL = "bert" -> variables["MODEL"] = "bert"
#   2. Backtrader params tuples: params = (('model_name', 'bert'),)
#   3. Function return values: def get_model(): return "bert"
#   4. Dictionary literals: config = {"model": "bert"}
#   5. List literals: models = ["bert", "gpt2"]
#
# This data is then used by ModelDetector to resolve expressions.
# =============================================================================

class VariableTracker(ast.NodeVisitor):
    """
    First-pass AST visitor that tracks variable assignments and values.

    This class performs a single traversal of the AST to build a mapping of
    variable names to their values. It handles several Python patterns that
    are commonly used to store model names indirectly.

    Tracked Data Structures:
    ------------------------
    variables: Dict[str, str]
        Simple variable -> value mappings.
        Example: MODEL_NAME = "bert" -> {"MODEL_NAME": "bert"}

    partial_variables: Dict[str, ast.expr]
        Variables assigned to complex expressions we couldn't resolve.
        We store the AST node so ModelDetector can try to resolve it later
        with additional context (like resolved function calls).

    class_params: Dict[str, Dict[str, str]]
        Backtrader-style class parameters.
        Example: params = (('model_name', 'bert'),)
        Stored as: {"ClassName": {"model_name": "bert"}}

    function_returns: Dict[str, Set[str]]
        Maps function names to their possible return values.
        Example: def get_model(): return "bert"
        Stored as: {"get_model": {"bert"}}
        Uses a Set because a function could have multiple return statements.

    dict_literals: Dict[str, Dict[str, str]]
        Maps variable names to dictionary contents.
        Example: config = {"model": "bert", "size": "base"}
        Stored as: {"config": {"model": "bert", "size": "base"}}

    list_literals: Dict[str, List[str]]
        Maps variable names to list contents.
        Example: models = ["bert", "gpt2", "t5"]
        Stored as: {"models": ["bert", "gpt2", "t5"]}

    Context Tracking:
    -----------------
    current_class: Optional[str]
        The name of the class we're currently inside (for method tracking).

    current_function: Optional[str]
        The fully-qualified name of the function we're currently inside.
        For methods, this is "ClassName.method_name".
    """

    def __init__(self):
        # Primary data structures for resolved values
        self.variables: Dict[str, str] = {}  # var_name -> resolved string value
        self.partial_variables: Dict[str, ast.expr] = {}  # var_name -> AST node (unresolved)
        self.class_params: Dict[str, Dict[str, str]] = {}  # class_name -> {param_name -> value}
        self.function_returns: Dict[str, Set[str]] = {}  # func_name -> set of return values
        self.dict_literals: Dict[str, Dict[str, str]] = {}  # var_name -> {key -> value}
        self.list_literals: Dict[str, List[str]] = {}  # var_name -> [values]

        # Context tracking for nested structures
        self.current_class: Optional[str] = None  # Which class we're currently in
        self.current_function: Optional[str] = None  # Which function we're currently in

    def visit_ClassDef(self, node: ast.ClassDef):
        """
        Track class definitions to properly scope method return values.

        When we enter a class, we:
        1. Save the class name for context
        2. Initialize storage for class-level params
        3. Visit all child nodes (methods, attributes, etc.)
        4. Restore the previous class context (for nested classes)

        This enables tracking of:
        - Backtrader params tuples at class level
        - Class method return values (stored as "ClassName.method_name")
        """
        old_class = self.current_class
        self.current_class = node.name
        self.class_params[node.name] = {}
        self.generic_visit(node)  # Visit all child nodes
        self.current_class = old_class  # Restore context for nested classes

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """
        Track function definitions to capture their return values.

        For each function, we:
        1. Build the fully-qualified name (including class if applicable)
        2. Initialize storage for return values
        3. Visit the function body to find return statements
        4. Restore context for nested functions

        The return values are captured by visit_Return() as we traverse
        the function body. We use a Set because a function may have
        multiple return statements with different values.

        Example tracking:
        - Top-level function: "get_model_name" -> {"bert"}
        - Class method: "ModelConfig.get_name" -> {"bert"}
        """
        # Build the full function name (including class if we're in one)
        if self.current_class:
            func_name = f"{self.current_class}.{node.name}"
        else:
            func_name = node.name

        # Save context and initialize tracking
        old_function = self.current_function
        self.current_function = func_name
        self.function_returns[func_name] = set()

        # Visit the function body to find return statements
        self.generic_visit(node)

        # Restore context for nested functions
        self.current_function = old_function

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        """
        Track async function definitions (same logic as regular functions).

        Async functions can also return model names, so we track them
        identically to regular functions.
        """
        # Reuse the same logic - async functions work the same way
        self.visit_FunctionDef(node)  # type: ignore

    def visit_Return(self, node: ast.Return):
        """
        Track return statements within functions.

        When we encounter a return statement, we try to resolve its value
        to a string. If successful, we add it to the function's set of
        possible return values.

        This enables resolution of patterns like:
            def get_model_name():
                return "bert-base-uncased"  # <- we capture "bert-base-uncased"

            model = AutoModel.from_pretrained(get_model_name())
        """
        if self.current_function and node.value:
            value = self._resolve_value(node.value)
            if value:
                self.function_returns[self.current_function].add(value)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        """
        Track variable assignments.

        This is the most common pattern for storing model names indirectly.
        We handle several cases:

        1. Simple string assignment:
           MODEL_NAME = "bert"
           -> variables["MODEL_NAME"] = "bert"

        2. Backtrader-style params tuple:
           params = (('model_name', 'bert'), ('lookback', 20))
           -> variables["self.params.model_name"] = "bert"

        3. Dictionary literal:
           config = {"model": "bert", "size": "base"}
           -> dict_literals["config"] = {"model": "bert", "size": "base"}

        4. List literal:
           models = ["bert", "gpt2"]
           -> list_literals["models"] = ["bert", "gpt2"]

        5. Unresolvable expression:
           model = get_dynamic_name()
           -> partial_variables["model"] = <AST node>
           (We save the node so ModelDetector can try to resolve it)
        """
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id

                # Case 2: Backtrader-style params tuple
                # Pattern: params = (('name', value), ('name2', value2), ...)
                # This is specific to the Backtrader framework used by trading agents
                if var_name == 'params' and isinstance(node.value, ast.Tuple):
                    self._parse_params_tuple(node.value)

                # Case 3: Dictionary literal
                # Pattern: config = {"model": "bert", "key": "value"}
                elif isinstance(node.value, ast.Dict):
                    self._parse_dict_literal(var_name, node.value)

                # Case 4: List literal
                # Pattern: models = ["bert", "gpt2", "t5"]
                elif isinstance(node.value, ast.List):
                    self._parse_list_literal(var_name, node.value)

                # Cases 1 & 5: Simple assignment or complex expression
                else:
                    value = self._resolve_value(node.value)
                    if value:
                        # Case 1: Successfully resolved to a string
                        self.variables[var_name] = value
                    else:
                        # Case 5: Couldn't resolve - save the AST node for later
                        self.partial_variables[var_name] = node.value

        self.generic_visit(node)

    def _parse_dict_literal(self, var_name: str, node: ast.Dict):
        """
        Parse a dictionary literal and store its string key-value pairs.

        Example:
            config = {"model": "bert", "tokenizer": "bert", "layers": 12}

        We only store entries where both key and value are strings.
        Non-string entries (like "layers": 12) are ignored.

        This enables resolution of patterns like:
            AutoModel.from_pretrained(config["model"])

        Note: We skip **kwargs spreads (where key is None) because we
        can't statically determine what keys they add.
        """
        self.dict_literals[var_name] = {}
        for key, value in zip(node.keys, node.values):
            if key is not None:  # Skip **kwargs spreads (key is None for these)
                key_str = self._resolve_value(key)
                val_str = self._resolve_value(value)
                if key_str and val_str:
                    self.dict_literals[var_name][key_str] = val_str

    def _parse_list_literal(self, var_name: str, node: ast.List):
        """
        Parse a list literal and store its string elements.

        Example:
            models = ["bert-base", "bert-large", "gpt2"]

        We only store string elements. Non-string elements are skipped.

        This enables resolution of patterns like:
            AutoModel.from_pretrained(models[0])  # -> "bert-base"
        """
        self.list_literals[var_name] = []
        for elt in node.elts:
            val_str = self._resolve_value(elt)
            if val_str:
                self.list_literals[var_name].append(val_str)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        """
        Track annotated assignments (type-hinted variables).

        Python 3.6+ allows type annotations on variables:
            model_name: str = "bert-base-uncased"

        This is equivalent to a regular assignment for our purposes.
        We extract the value just like in visit_Assign().
        """
        if isinstance(node.target, ast.Name) and node.value:
            value = self._resolve_value(node.value)
            if value:
                self.variables[node.target.id] = value
        self.generic_visit(node)

    def _parse_params_tuple(self, node: ast.Tuple):
        """
        Parse Backtrader-style params tuple.

        Backtrader (a popular trading framework) uses a specific pattern
        for class parameters:

            class MyStrategy(bt.Strategy):
                params = (
                    ('lookback', 20),           # numeric param
                    ('model_name', 'amazon/chronos-t5-small'),  # string param
                    ('threshold', 0.5),         # numeric param
                )

                def __init__(self):
                    # Access via self.params.model_name
                    self.model = load_model(self.params.model_name)

        We parse these tuples and store string values as:
            variables["self.params.model_name"] = "amazon/chronos-t5-small"

        This enables resolution when we later see:
            AutoModel.from_pretrained(self.params.model_name)
        """
        for elt in node.elts:
            # Each element should be a 2-tuple: (name, default_value)
            if isinstance(elt, ast.Tuple) and len(elt.elts) >= 2:
                name_node = elt.elts[0]
                value_node = elt.elts[1]

                # Name must be a string constant
                if isinstance(name_node, ast.Constant) and isinstance(name_node.value, str):
                    param_name = name_node.value
                    param_value = self._resolve_value(value_node)

                    if param_value:
                        # Store as "self.params.{param_name}" for attribute access resolution
                        key = f"self.params.{param_name}"
                        self.variables[key] = param_value

                        # Also store for class-level lookup if we're in a class
                        if self.current_class:
                            self.class_params[self.current_class][param_name] = param_value

    def _resolve_value(self, node: ast.expr) -> Optional[str]:
        """
        Try to resolve an AST node to a simple string value.

        This is a simple resolver used during the first pass. It only
        handles the most basic case: direct string literals.

        Returns:
            The string value if node is a string constant, None otherwise.

        More complex resolution (f-strings, concatenation, etc.) is
        handled by ModelDetector._resolve_expression() in the second pass,
        which has access to all the tracked variables.
        """
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None


# =============================================================================
# SECOND PASS: MODEL DETECTION
# =============================================================================
#
# After VariableTracker has collected all variable values, function returns,
# dictionary literals, and list literals, ModelDetector walks the AST again
# to find model loading calls and resolve their arguments.
#
# For each call matching a pattern in LOADING_PATTERNS, PIPELINE_PATTERNS,
# or INSTANTIATION_PATTERNS, we try to resolve the model name argument to
# a concrete string value.
# =============================================================================

class ModelDetector(ast.NodeVisitor):
    """
    Second-pass AST visitor that detects model loading calls.

    This class finds calls to from_pretrained(), pipeline(), and
    direct class instantiation for known model-loading patterns.
    For each call found, it attempts to resolve the model name
    argument to a concrete string value.

    Initialization Data:
    --------------------
    All data comes from VariableTracker's first pass:

    variables: Dict[str, str]
        Simple variable name -> value mappings.

    partial_variables: Dict[str, ast.expr]
        Variables with complex expressions that couldn't be resolved
        in the first pass. We try again with the expression resolver.

    function_returns: Dict[str, Set[str]]
        Function name -> set of possible return values.
        Used to resolve function calls like get_model_name().

    dict_literals: Dict[str, Dict[str, str]]
        Dictionary variable -> {key: value} mappings.
        Used to resolve config["model"] patterns.

    list_literals: Dict[str, List[str]]
        List variable -> [values] mappings.
        Used to resolve models[0] patterns.

    Output:
    -------
    models: Set[str]
        All successfully resolved model names.

    unresolved: List[Dict]
        Calls where we couldn't resolve the model name.
        Each entry contains:
        - call: The function/method name
        - line: Line number in source file
        - expression: AST dump of the unresolvable expression
        - hint: Human-readable hint about what to do
    """

    def __init__(self, variables: Dict[str, str], partial_variables: Dict[str, ast.expr],
                 function_returns: Dict[str, Set[str]],
                 dict_literals: Dict[str, Dict[str, str]],
                 list_literals: Dict[str, List[str]]):
        # Data from first pass (VariableTracker)
        self.variables = variables
        self.partial_variables = partial_variables
        self.function_returns = function_returns
        self.dict_literals = dict_literals
        self.list_literals = list_literals

        # Output collections
        self.models: Set[str] = set()  # Successfully resolved model names
        self.unresolved: List[Dict] = []  # Calls we couldn't resolve

        # Build lookup table for faster pattern matching during AST traversal
        # Key: (class_name, method_name) -> Value: argument position for model name
        self.method_patterns = {}
        for class_name, method, arg_pos in LOADING_PATTERNS:
            key = (class_name, method)
            self.method_patterns[key] = arg_pos

    def visit_Call(self, node: ast.Call):
        """
        Visit function/method calls to detect model loading.

        This method is called for every function call in the AST.
        We check each call against our known patterns:

        1. Method calls: ClassName.from_pretrained(...)
           - Get the class name and method name
           - Look up in method_patterns
           - Extract model name from the specified argument position

        2. Direct function calls: pipeline(...) or ClassName(...)
           - Check for pipeline() calls -> extract model from args/kwargs
           - Check for instantiation patterns -> extract from first arg

        After processing, we continue traversing child nodes with generic_visit().
        """

        # =================================================================
        # Pattern Type 1: Method calls like ClassName.from_pretrained(...)
        # =================================================================
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr  # e.g., "from_pretrained"

            # Get the class/object name (e.g., "AutoModel" from "AutoModel.from_pretrained")
            class_name = self._get_call_base(node.func.value)

            if class_name:
                key = (class_name, method_name)
                if key in self.method_patterns:
                    arg_pos = self.method_patterns[key]
                    self._extract_model_from_call(node, arg_pos, f"{class_name}.{method_name}")

        # =================================================================
        # Pattern Type 2: Direct function/class calls
        # =================================================================
        if isinstance(node.func, ast.Name):
            func_name = node.func.id

            # Check for pipeline() calls - HuggingFace's convenience wrapper
            if func_name == "pipeline":
                self._extract_from_pipeline(node)

            # Check for direct instantiation patterns (e.g., SentenceTransformer("model"))
            if func_name in INSTANTIATION_PATTERNS:
                self._extract_model_from_call(node, 0, func_name)

        # Continue traversing the AST
        self.generic_visit(node)

    def _get_call_base(self, node: ast.expr) -> Optional[str]:
        """
        Extract the base name from a method call expression.

        For "AutoModel.from_pretrained", node is the "AutoModel" part.

        Handles:
        - Simple names: AutoModel -> "AutoModel"
        - Chained attributes: transformers.AutoModel -> "AutoModel"

        Returns the last name in the chain, which is the class name we care about.
        """
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # For chained like transformers.AutoModel, we want "AutoModel"
            return node.attr
        return None

    def _extract_model_from_call(self, node: ast.Call, arg_pos: int, call_name: str):
        """
        Extract the model name from a from_pretrained() style call.

        Arguments:
        ----------
        node: ast.Call
            The function call AST node.

        arg_pos: int
            The position of the model name argument (0-indexed).
            Usually 0, but for PeftModel it's 1 (adapter is second arg).

        call_name: str
            Human-readable name for error reporting (e.g., "AutoModel.from_pretrained").

        Resolution Strategy:
        -------------------
        1. Check if positional argument at arg_pos exists
        2. Check keyword arguments for common model name kwargs:
           - pretrained_model_name_or_path (HuggingFace standard)
           - model_name (common alternative)
           - model_id (used by some libraries)
           - repo_id (HuggingFace Hub terminology)
        3. Try to resolve the argument expression to a string
        4. If resolution fails, add to unresolved list with helpful hints
        """
        model_arg = None

        # Strategy 1: Check positional argument at the expected position
        if len(node.args) > arg_pos:
            model_arg = node.args[arg_pos]

        # Strategy 2: Check keyword arguments for common model name kwargs
        # (keyword args take precedence if both positional and keyword are present)
        for kw in node.keywords:
            if kw.arg in ("pretrained_model_name_or_path", "model_name", "model_id", "repo_id"):
                model_arg = kw.value
                break

        if model_arg:
            # Try to resolve the expression to a concrete string
            resolved = self._resolve_expression(model_arg)
            if resolved:
                self.models.add(resolved)
            else:
                # Couldn't resolve - add to unresolved with debugging info
                self.unresolved.append({
                    "call": call_name,
                    "line": node.lineno,
                    "expression": ast.dump(model_arg),  # Raw AST for debugging
                    "hint": self._get_hint(model_arg)  # Human-readable hint
                })

    def _extract_from_pipeline(self, node: ast.Call):
        """
        Extract model name from pipeline() calls.

        HuggingFace's pipeline() can specify models two ways:

        1. Keyword argument:
           pipeline("text-generation", model="gpt2")

        2. Second positional argument:
           pipeline("text-generation", "gpt2")

        The first positional argument is always the task name.
        """
        # Strategy 1: Check 'model' keyword argument (preferred, explicit)
        for kw in node.keywords:
            if kw.arg == "model":
                resolved = self._resolve_expression(kw.value)
                if resolved:
                    self.models.add(resolved)
                return  # Found model kwarg, done

        # Strategy 2: Check second positional argument (shorthand form)
        if len(node.args) >= 2:
            resolved = self._resolve_expression(node.args[1])
            if resolved:
                self.models.add(resolved)

    def _resolve_expression(self, node: ast.expr) -> Optional[str]:
        """
        Resolve an AST expression to a string value.

        This is the core resolution logic that handles many Python patterns.
        Each pattern is checked in order, and we return as soon as we
        successfully resolve to a string.

        Supported Patterns:
        ------------------
        1. Direct string literals: "bert-base-uncased"

        2. Variable references: MODEL_NAME
           -> Look up in self.variables

        3. Attribute access: self.params.model_name
           -> Build attribute chain, look up in self.variables

        4. Subscript access: config["model"] or models[0]
           -> Look up in dict_literals or list_literals

        5. F-strings: f"amazon/{model_type}"
           -> Resolve each component, join together

        6. String concatenation: "amazon/" + model
           -> Resolve left and right, concatenate

        7. str.format() calls: "{}/{}".format(org, name)
           -> Resolve template and args, apply formatting

        8. Function calls: get_model_name()
           -> Look up in function_returns

        Returns:
        --------
        The resolved string value, or None if resolution failed.
        """

        # -----------------------------------------------------------------
        # Pattern 1: Direct string literal
        # Example: "bert-base-uncased"
        # -----------------------------------------------------------------
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value

        # -----------------------------------------------------------------
        # Pattern 2: Variable reference
        # Example: MODEL_NAME -> look up in variables dict
        # -----------------------------------------------------------------
        if isinstance(node, ast.Name):
            # First check resolved variables
            if node.id in self.variables:
                return self.variables[node.id]
            # Then check partial variables (try to resolve the saved AST node)
            if node.id in self.partial_variables:
                return self._resolve_expression(self.partial_variables[node.id])
            return None

        # -----------------------------------------------------------------
        # Pattern 3: Attribute access
        # Example: self.params.model_name -> "self.params.model_name"
        # -----------------------------------------------------------------
        if isinstance(node, ast.Attribute):
            attr_chain = self._get_attribute_chain(node)
            if attr_chain:
                # Check if we have this exact attribute chain tracked
                if attr_chain in self.variables:
                    return self.variables[attr_chain]
            return None

        # -----------------------------------------------------------------
        # Pattern 4: Subscript access
        # Example: config["model"] or model_list[0]
        # -----------------------------------------------------------------
        if isinstance(node, ast.Subscript):
            return self._resolve_subscript(node)

        # -----------------------------------------------------------------
        # Pattern 5: F-string (JoinedStr in AST)
        # Example: f"amazon/{model_type}"
        # -----------------------------------------------------------------
        if isinstance(node, ast.JoinedStr):
            return self._resolve_fstring(node)

        # -----------------------------------------------------------------
        # Pattern 6: String concatenation with + operator
        # Example: "amazon/" + model_name
        # -----------------------------------------------------------------
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self._resolve_expression(node.left)
            right = self._resolve_expression(node.right)
            if left and right:
                return left + right
            return None

        # -----------------------------------------------------------------
        # Pattern 7: str.format() call
        # Example: "{}/{}".format("amazon", model)
        # -----------------------------------------------------------------
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "format":
                return self._resolve_format_call(node)

        # -----------------------------------------------------------------
        # Pattern 8: Function call (check if we know its return value)
        # Example: get_model_name() -> look up in function_returns
        # -----------------------------------------------------------------
        if isinstance(node, ast.Call):
            return self._resolve_function_call(node)

        # Unknown pattern - can't resolve
        return None

    def _resolve_subscript(self, node: ast.Subscript) -> Optional[str]:
        """
        Resolve subscript access (dictionary or list indexing).

        Handles two patterns:

        1. Dictionary access: config["model"]
           - Look up var_name in dict_literals
           - Look up the key in that dictionary

        2. List access: models[0]
           - Look up var_name in list_literals
           - Index into the list

        Limitations:
        - Only handles simple variable names (not chained like obj.config["model"])
        - Only handles literal string keys or integer indices
        - Negative indices work if within bounds
        """
        # We only handle simple variable subscripts like: var_name[key]
        if not isinstance(node.value, ast.Name):
            return None

        var_name = node.value.id
        slice_node = node.slice

        # -----------------------------------------------------------------
        # Dictionary access: config["model"]
        # slice_node is a string constant
        # -----------------------------------------------------------------
        if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, str):
            key = slice_node.value
            if var_name in self.dict_literals and key in self.dict_literals[var_name]:
                return self.dict_literals[var_name][key]

        # -----------------------------------------------------------------
        # List access: models[0] or models[-1]
        # slice_node is an integer constant
        # -----------------------------------------------------------------
        if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, int):
            index = slice_node.value
            if var_name in self.list_literals:
                lst = self.list_literals[var_name]
                # Handle both positive and negative indices
                if -len(lst) <= index < len(lst):
                    return lst[index]

        return None

    def _resolve_function_call(self, node: ast.Call) -> Optional[str]:
        """
        Resolve a function call by looking up its return value(s).

        If VariableTracker recorded return statements for this function,
        we can determine what value the function returns.

        Handles:
        - Simple function calls: get_model_name()
        - Method calls: ModelConfig.get_name()

        When a function has multiple possible return values:
        - We add ALL of them to self.models (to be safe)
        - We return the first one (for the immediate resolution)

        This ensures we don't miss any models that might be returned
        based on runtime conditions we can't analyze.
        """
        func_name = None

        # -----------------------------------------------------------------
        # Simple function call: func_name()
        # -----------------------------------------------------------------
        if isinstance(node.func, ast.Name):
            func_name = node.func.id

        # -----------------------------------------------------------------
        # Method call: ClassName.method_name() or instance.method_name()
        # -----------------------------------------------------------------
        elif isinstance(node.func, ast.Attribute):
            # Get the full attribute chain as the function name
            attr_chain = self._get_attribute_chain(node.func)
            if attr_chain:
                func_name = attr_chain

        # Check if we tracked this function's return values
        if func_name and func_name in self.function_returns:
            returns = self.function_returns[func_name]
            if len(returns) == 1:
                # Single return value - simple case
                return list(returns)[0]
            elif len(returns) > 1:
                # Multiple possible return values (e.g., conditional returns)
                # Add all of them to be safe - we can't know which path runs
                for ret in returns:
                    self.models.add(ret)
                return list(returns)[0]  # Return first for immediate use

        return None

    def _get_attribute_chain(self, node: ast.Attribute) -> Optional[str]:
        """
        Build a dotted string from a chain of attribute accesses.

        Example:
            self.params.model_name
                  ^--- ast.Attribute(attr="model_name")
                       ^--- ast.Attribute(attr="params")
                            ^--- ast.Name(id="self")

        Returns: "self.params.model_name"

        This enables matching against variables tracked as
        "self.params.model_name" by VariableTracker.
        """
        parts = []
        current = node

        # Walk up the attribute chain, collecting names
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value

        # The final node should be a Name (the leftmost identifier)
        if isinstance(current, ast.Name):
            parts.append(current.id)
            parts.reverse()  # Reverse to get left-to-right order
            return ".".join(parts)

        return None

    def _resolve_fstring(self, node: ast.JoinedStr) -> Optional[str]:
        """
        Resolve an f-string to its concrete value.

        F-strings in the AST are represented as JoinedStr nodes containing:
        - Constant nodes for literal string parts
        - FormattedValue nodes for interpolated expressions

        Example:
            f"amazon/{model_type}"

        AST structure:
            JoinedStr(values=[
                Constant(value="amazon/"),
                FormattedValue(value=Name(id="model_type"))
            ])

        We resolve each component and join them together.
        If ANY component fails to resolve, we return None.
        """
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                # Literal string part - use directly
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                # Interpolated expression - try to resolve it
                resolved = self._resolve_expression(value.value)
                if resolved:
                    parts.append(resolved)
                else:
                    # Can't resolve this part - whole f-string fails
                    return None
            else:
                # Unexpected node type
                return None
        return "".join(parts)

    def _resolve_format_call(self, node: ast.Call) -> Optional[str]:
        """
        Resolve a str.format() call.

        Example:
            "{org}/{model}".format(org="amazon", model="chronos-t5-small")
            -> "amazon/chronos-t5-small"

        Supports both positional and keyword arguments:
        - Positional: "{}-{}".format("amazon", "chronos") -> "amazon-chronos"
        - Keyword: "{org}-{model}".format(org="a", model="b") -> "a-b"
        - Mixed: "{0}-{name}".format("a", name="b") -> "a-b"

        If any argument fails to resolve, or the format() call fails,
        we return None.
        """
        if not isinstance(node.func, ast.Attribute):
            return None

        # Resolve the template string (the object .format() is called on)
        template = self._resolve_expression(node.func.value)
        if not template:
            return None

        # Resolve positional arguments
        args = []
        for arg in node.args:
            resolved = self._resolve_expression(arg)
            if resolved is None:
                return None  # Can't resolve an arg - fail
            args.append(resolved)

        # Resolve keyword arguments
        kwargs = {}
        for kw in node.keywords:
            if kw.arg:  # Skip **kwargs spreads (arg is None)
                resolved = self._resolve_expression(kw.value)
                if resolved is None:
                    return None  # Can't resolve a kwarg - fail
                kwargs[kw.arg] = resolved

        # Apply the formatting
        try:
            return template.format(*args, **kwargs)
        except (IndexError, KeyError):
            # format() failed (wrong number of args, missing key, etc.)
            return None

    def _get_hint(self, node: ast.expr) -> str:
        """
        Generate a human-readable hint for unresolved expressions.

        When we can't resolve a model name, we provide a hint to help
        users understand what went wrong and how to fix it (usually
        by adding the model to a models.txt file).

        The hint describes the type of expression we encountered so
        users can understand why static analysis couldn't determine
        the value.
        """
        if isinstance(node, ast.Name):
            return f"Variable '{node.id}' - add to models.txt"
        elif isinstance(node, ast.JoinedStr):
            return "F-string with unresolved variable"
        elif isinstance(node, ast.BinOp):
            return "String concatenation with unresolved variable"
        elif isinstance(node, ast.Call):
            return "Dynamic function call"
        return "Complex expression"


# =============================================================================
# PUBLIC API FUNCTIONS
# =============================================================================
#
# These functions provide the main entry points for using the model detector.
# They handle file I/O and orchestrate the two-pass analysis.
# =============================================================================

def detect_models_in_file(filepath: str) -> Tuple[Set[str], List[Dict]]:
    """
    Detect model dependencies in a single Python file.

    This is the primary analysis function. It:
    1. Reads the source file
    2. Parses it into an AST
    3. Runs VariableTracker (first pass) to collect variable values
    4. Runs ModelDetector (second pass) to find and resolve model names

    Arguments:
    ----------
    filepath: str
        Path to the Python file to analyze.

    Returns:
    --------
    Tuple of:
    - Set[str]: Resolved model names (can be used to download models)
    - List[Dict]: Unresolved calls (need manual handling via models.txt)

    Error Handling:
    ---------------
    - File read errors: Raises the underlying exception
    - Syntax errors: Prints warning to stderr, returns empty results
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        source = f.read()

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error in {filepath}: {e}", file=sys.stderr)
        return set(), []

    # First pass: Collect variable values, function returns, dicts, lists
    var_tracker = VariableTracker()
    var_tracker.visit(tree)

    # Second pass: Find model loading calls and resolve their arguments
    detector = ModelDetector(
        var_tracker.variables,
        var_tracker.partial_variables,
        var_tracker.function_returns,
        var_tracker.dict_literals,
        var_tracker.list_literals
    )
    detector.visit(tree)

    return detector.models, detector.unresolved


def detect_models_in_directory(dirpath: str) -> Tuple[Set[str], List[Dict]]:
    """
    Detect model dependencies in all Python files in a directory.

    Recursively walks the directory tree, analyzing each .py file.
    Results from all files are combined into single result sets.

    Arguments:
    ----------
    dirpath: str
        Path to the directory to analyze.

    Returns:
    --------
    Tuple of:
    - Set[str]: All resolved model names across all files
    - List[Dict]: All unresolved calls across all files
                  (each entry includes 'file' key with the source file path)

    Excluded Directories:
    --------------------
    Common non-source directories are skipped:
    - venv, .venv: Python virtual environments
    - __pycache__: Bytecode cache
    - .git: Git repository data
    - node_modules: Node.js dependencies (in case of mixed projects)
    """
    all_models = set()
    all_unresolved = []

    for root, dirs, files in os.walk(dirpath):
        # Exclude common non-code directories (modifies dirs in-place)
        dirs[:] = [d for d in dirs if d not in ('venv', '.venv', '__pycache__', '.git', 'node_modules')]

        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                models, unresolved = detect_models_in_file(filepath)

                # Accumulate results
                all_models.update(models)

                # Add file path to each unresolved entry for context
                for u in unresolved:
                    u['file'] = filepath
                all_unresolved.extend(unresolved)

    return all_models, all_unresolved


# =============================================================================
# COMMAND-LINE INTERFACE
# =============================================================================
#
# When run as a script, this tool accepts a file or directory path and
# outputs results in JSON format to stdout (for machine consumption) and
# a human-readable summary to stderr.
# =============================================================================

def main():
    """
    Command-line entry point.

    Usage:
        python detect_models.py <file_or_directory>

    Output:
        - JSON to stdout with "models" and "unresolved" keys
        - Summary to stderr with counts and hints

    Exit Codes:
        - 0: Success
        - 1: Invalid arguments or invalid target path
    """
    if len(sys.argv) < 2:
        print("Usage: python detect_models.py <file_or_directory>", file=sys.stderr)
        sys.exit(1)

    target = sys.argv[1]

    # Determine if target is a file or directory and analyze accordingly
    if os.path.isfile(target):
        models, unresolved = detect_models_in_file(target)
    elif os.path.isdir(target):
        models, unresolved = detect_models_in_directory(target)
    else:
        print(f"Error: {target} is not a valid file or directory", file=sys.stderr)
        sys.exit(1)

    # Output cache names to stdout (one per line, for easy use with xargs/gsutil)
    # Format: models--amazon--chronos-t5-small
    for model in sorted(models):
        cache_name = f"models--{model.replace('/', '--')}"
        print(cache_name)

    # Print human-readable summary to stderr
    print(f"\n=== Summary ===", file=sys.stderr)
    print(f"Detected {len(models)} model(s):", file=sys.stderr)
    for model in sorted(models):
        print(f"  - {model}", file=sys.stderr)

    if unresolved:
        print(f"\n{len(unresolved)} unresolved call(s):", file=sys.stderr)
        for u in unresolved:
            file_info = u.get('file', 'unknown')
            print(f"  - {file_info}:{u['line']} {u['call']}: {u['hint']}", file=sys.stderr)
        print("\nAdd unresolved models to a 'models.txt' file.", file=sys.stderr)


if __name__ == "__main__":
    main()
