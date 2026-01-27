class UserExecutionContext:
    def __init__(self, user_id: str):
        """Context for processing a single user's data through the orchestrator.
        Attributes:
            user_id (str): The USERID of the user being processed.
            payloads (dict): A dictionary to hold generated payloads for different entities.
            errors (list): A list to collect error messages encountered during processing.
            warnings (list): A list to collect warning messages encountered during processing.
            builders (dict): A dictionary to hold payload builder instances for different entities.
            runtime (dict): A dictionary for storing runtime information during processing. Like entity_status and others,
            that's very important for dependency management between entities and processing steps.
            dirty_entities (set): A set of entity names that require updating for this user, it's filled of field changes detected.
            is_update (bool): Flag indicating whether the user is being updated (True) or created (False).
            This affects payload generation and entity processing logic including handling of dependencies.
        """
        self.user_id = user_id
        self.payloads = {}
        self.errors = []
        self.warnings = []
        self.builders = {}
        self.runtime = {}
        self.is_scm = False
        self.is_im = False
        self.ec_user_id = None
        self.dummy_position = None
        self.has_existing_empjob = False
        self.empjob_start_date = None
        self.position_code = None  # Store position code for employment processing
        self.dirty_entities = set()
        self.is_update = False

    def fail(self, msg: str):
        """Record an error message and mark the context as failed."""
        self.errors.append(msg)

    def warn(self, msg: str):
        """Record a warning message."""
        self.warnings.append(msg)

    def ok(self, entity: str, status: dict):
        """Mark the processing of an entity as successful with its status."""
        self.runtime.setdefault("entity_status", {})[entity] = status

    @property
    def has_errors(self):
        """Check if there are any recorded errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self):
        """Check if there are any recorded warnings."""
        return len(self.warnings) > 0
