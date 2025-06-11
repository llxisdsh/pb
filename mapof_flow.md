## Technical Features

- CLHT Cache Line Alignment  
  vent false sharing through padded structs (cache-line aligned), with automatic cache line size detection
- SWAR Technology  
  Accelerate metadata matching via bitwise-SIMD techniques (SWAR algorithm optimized through word-level parallelism), 
  delivering hardware-agnostic performance rivaling native SIMD instructions
- Lazy Initialization  
  Avoid unnecessary memory allocations through on-demand initialization
- Lock-Free Reads  
  Fully lock-free Load operations for maximum read performance
- Bucket-Level Locking  
  Write operations only acquire per-bucket locks, minimizing contention through fine-grained synchronization with atomic write abstraction
- Optimized Spinlocks  
  Replace default locks with high-efficiency spinlocks, reducing false sharing
- Fast-Path Filter for Writes  
  Pre-write read-only fast checks act as prefetch and branch pruning
- Parallel Progressive Resizing  
  Large tables resize via parallel multi-goroutines element copying, assisted only by user-operation goroutines
- High-Performance Hashing  
  Utilize Go's most efficient built-in hash algorithm, with special optimizations for integer keys


## Function Analysis

### Load Operation
```mermaid
flowchart TD
	subgraph Initialization Check
		A[Start Load] --> B{Is table nil?}
		B -->|Yes| C[Return zero-value and false]
	end

	subgraph Hash Computation
		B -->|No| D[Compute key hash]
		D --> E[Calculate h2 and broadcast value]
		E --> F[Determine bucket index]
	end

	subgraph Bucket Traversal
		F --> G[Acquire root bucket]
		G --> H[Traverse bucket chain]
		H --> I{Is current bucket nil?}
		I -->|Yes| J[Return zero-value and false]
	end

	subgraph SWAR Matching
		I -->|No| K[Load bucket metadata]
		K --> L[Find matching metadata bits using SWAR]
		L --> M{Match found?}
		M -->|No| N[Continue to next bucket]
		N --> H
		M -->|Yes| O[Iterate matched entries]
		O --> P{Does entry.Key == key?}
		P -->|No| Q[Continue to next match]
		Q --> O
		P -->|Yes| R[Return entry.Value and true]
	end
```

### Store Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start Store] --> B{Is table nil?}
		B -->|Yes| C[Initialize via initSlow]
		B -->|No| D[Compute key hash]
		C --> D
	end

	subgraph Fast Path Check
		D --> E{Is enableFastPath on?}
		E -->|No| F[Invoke mockSyncMap]
		E -->|Yes| G{Does valEqual exist?}
		G -->|No| F
	end

	subgraph Value Update Logic
		G -->|Yes| H[Locate entry via findEntry]
		H --> I{Entry found?}
		I -->|No| F
		I -->|Yes| J{New value == existing?}
		J -->|Yes| K[Return without update]
		J -->|No| F
	end

	subgraph Final Processing
		F --> L[Process entry via processEntry]
		L --> M[End]
		K --> M
	end
```

### mockSyncMap Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start mockSyncMap] --> B[Invoke processEntry]
		B --> C[Pass callback handler]
	end

	subgraph Entry Exists Branch
		C --> D{Does loaded entry exist?}
		D -->|Yes| E{Is LoadOrStore operation?}
		E -->|Yes| F[Return existing value and true]
		E -->|No| G{Does cmpValue mismatch?}
		G -->|Yes| H[Return existing value and false]
		G -->|No| I{Is newValue nil?}
		I -->|Yes| J[Delete: return nil]
		I -->|No| K[Update: create new EntryOf]
		K --> L[Return new entry and existing value]
	end

	subgraph Entry Absent Branch
		D -->|No| M{Is newValue nil or cmpValue exists?}
		M -->|Yes| N[Return zero-value and false]
		M -->|No| O[Insert: create new EntryOf]
		O --> P{Is LoadOrStore?}
		P -->|Yes| Q[Return new entry and value, false]
		P -->|No| R[Return new entry and zero-value, false]
	end

	subgraph Termination
		F --> S[End]
		H --> S
		J --> S
		L --> S
		N --> S
		Q --> S
		R --> S
	end
```

### processEntry Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start processEntry] --> B[Compute hash and metadata]
		B --> C[Determine bucket index]
		C --> D[Acquire root bucket]
		D --> E[Lock root bucket]
	end

	subgraph Table State Check
		E --> F{Is resize in progress?}
		F -->|Yes| G[Unlock bucket]
		G --> H[Invoke helpCopyAndWait]
		H --> I[Reload latest table]
		I --> B
		F -->|No| J{Was table replaced?}
		J -->|Yes| K[Unlock bucket]
		K --> L[Update table reference]
		L --> B
	end

	subgraph Entry Processing
		J -->|No| M[Locate key in bucket chain]
		M --> N[Execute callback handler]
		N --> O{Does oldEntry exist?}
		O -->|Yes| P{Is newEntry == oldEntry?}
		P -->|Yes| Q[Unlock and return]
		P -->|No| R{Is newEntry non-nil?}
		R -->|Yes| S[Update entry]
		S --> T[Unlock and return]
		R -->|No| U[Delete entry]
		U --> V[Unlock bucket]
		V --> W[Decrement counter]
		W --> X{Need shrink?}
		X -->|Yes| Y[Invoke tryResize]
		X -->|No| Z[Return result]
		Y --> Z
	end

	subgraph New Entry Handling
		O -->|No| AA{Is newEntry nil?}
		AA -->|Yes| AB[Unlock and return]
		AA -->|No| AC{Has empty slot?}
		AC -->|Yes| AD[Insert into slot]
		AD --> AE[Unlock bucket]
		AE --> AF[Increment counter]
		AF --> AG[Return result]
		AC -->|No| AH[Create new bucket]
		AH --> AI[Unlock bucket]
		AI --> AJ[Increment counter]
		AJ --> AK{Need grow?}
		AK -->|Yes| AL[Invoke tryResize]
		AK -->|No| AM[Return result]
		AL --> AM
	end
```

### tryResize Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start tryResize] --> B[Create resizeState]
		B --> C{CAS set resizeState?}
	end

	subgraph State Validation
		C -->|No| D[Return false]
		C -->|Yes| E{Was table replaced?}
		E -->|Yes| F[Cleanup resizeState]
		F --> G[Return false]
	end

	subgraph Resize Logic
		E -->|No| H{Is clear operation?}
		H -->|Yes| I[Create new table]
		I --> J[Replace table]
		J --> K[Cleanup resizeState]
		K --> L[Return true]
		H -->|No| M{Exceeds threshold && multicore?}
		M -->|Yes| N[Async finalizeResize]
		N --> O[Return true]
		M -->|No| P[Sync finalizeResize]
		P --> Q[Return true]
	end
```

### finalizeResize Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start finalizeResize] --> B[Create new table]
	end

	subgraph Type Handling
		B --> C[Store table in resizeState]
		C --> D{Grow or Shrink?}
		D -->|Grow| E[Increment grow count]
		D -->|Shrink| F[Increment shrink count]
	end

	subgraph Completion
		E --> G[Invoke helpCopyAndWait]
		F --> G
	end
```

### helpCopyAndWait Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start helpCopyAndWait] --> B[Acquire src/dst tables]
		B --> C[Calculate chunk info]
	end

	subgraph Chunk Processing
		C --> D{Grow or Shrink?}
		D --> E[Process chunks iteratively]
		E --> F{More chunks?}
		F -->|No| G[Await completion]
		G --> H[Return]
		F -->|Yes| I[Calculate chunk range]
		I --> J{Is grow?}
		J -->|Yes| K[Invoke copyBucketOf]
		J -->|No| L[Invoke copyBucketOfLock]
		K --> M{All chunks processed?}
		L --> M
	end

	subgraph Finalization
		M -->|Yes| N[Update table reference]
		N --> O[Cleanup resizeState]
		O --> P[Mark complete]
		P --> H
		M -->|No| E
	end
```

### copyBucketOf / copyBucketOfLock Operation
```mermaid
flowchart TD
	subgraph Initialization
		A[Start copy] --> B[Init counters]
		B --> C[Iterate source buckets]
	end

	subgraph Bucket Processing
		C --> D[Lock source bucket]
		D --> E[Traverse bucket chain]
		E --> F[Iterate bucket entries]
		F --> G{Is entry valid?}
		G -->|No| H[Continue]
		G -->|Yes| I[Calculate target index]
		I --> J{Is copyBucketOfLock?}
		J -->|Yes| K[Lock target]
		J -->|No| L[Skip locking]
		K --> M[Find empty slot]
		L --> M
	end

	subgraph Insertion Logic
		M --> N{Found slot?}
		N -->|Yes| O[Insert into slot]
		N -->|No| P[Create new bucket]
		O --> Q{Is copyBucketOfLock?}
		P --> Q
		Q -->|Yes| R[Unlock target]
		Q -->|No| S[Continue]
		R --> S
		S --> T[Update counters]
		T --> U[Next entry]
	end

	subgraph Completion
		U --> F
		H --> F
		F --> V[Unlock source]
		V --> W[Next bucket]
		W --> C
		C --> X{Copied entries > 0?}
		X -->|Yes| Y[Update table counter]
		X -->|No| Z[Return]
		Y --> Z
	end
```