import os
import torch
from datasets import Dataset
from transformers import AutoModelForCausalLM, AutoTokenizer, TrainingArguments
from trl import SFTTrainer
from peft import LoraConfig, get_peft_model

# --- Configuration ---
MODEL_NAME = "meta-llama/Llama-3-8B-Instruct"  # or "meta-llama/Llama-3-1B-Instruct" if 1B is intended. Double check the exact model name.
DATA_FILE_PATH = "training_data.txt"  # Path to your text file
OUTPUT_DIR = "llama3-fine-tuned"
SFT_OUTPUT_SUBDIR = "sft_output"
QLORA_OUTPUT_SUBDIR = "qlora_output"
MAX_SEQ_LENGTH = 2048  # Adjust based on your hardware and model capabilities. Llama 3 supports 8k context.
LEARNING_RATE = 2e-4  # Adjust as needed, smaller for smaller datasets
BATCH_SIZE = 8  # Adjust based on your GPU memory, can be smaller for 1B model.
GRADIENT_ACCUMULATION_STEPS = 2 # Effective batch size = BATCH_SIZE * GRADIENT_ACCUMULATION_STEPS
NUM_EPOCHS = 3  # Adjust epochs for smaller datasets, avoid overfitting.
LORA_RANK = 8  # QLoRA parameter, adjust if needed
LORA_ALPHA = 16 # QLoRA parameter, adjust if needed
LORA_DROPOUT = 0.05 # QLoRA parameter, adjust if needed
TARGET_MODULES = ["q_proj", "v_proj", "up_proj", "down_proj", "gate_proj", "k_proj", "o_proj"] # Common for Llama models, verify if needed for Llama 3.2 1B

# --- Check for GPU ---
device_map = "auto"
world_size = int(os.environ.get("WORLD_SIZE", 1))
ddp = world_size != 1
if ddp:
    device_map = {"": int(os.environ.get("LOCAL_RANK") or 0)}
    GRADIENT_ACCUMULATION_STEPS = GRADIENT_ACCUMULATION_STEPS // world_size

# --- Helper Functions ---
def format_llama3_prompt(instruction, response=None):
    """Formats the instruction and response into the Llama 3.2 prompt format."""
    prompt = "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n" + instruction + "\n<|start_header_id|>assistant<|end_header_id|>\n"
    if response:
        prompt += response + "<|eot_id|>"
    else:
        prompt += "" # For inference/generation, no response yet
    return prompt

def read_training_data(file_path):
    """Reads the training data from the text file and formats it."""
    instructions = []
    responses = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue # Skip empty lines
            parts = line.split('\t') # Assuming tab-separated, adjust if different delimiter
            if len(parts) != 2:
                print(f"Warning: Skipping line due to incorrect format: {line}") # Handle cases where format is wrong
                continue
            instruction, response = parts
            instructions.append(instruction.strip())
            responses.append(response.strip())

    formatted_prompts = []
    for instruction, response in zip(instructions, responses):
        formatted_prompts.append(format_llama3_prompt(instruction, response))

    return formatted_prompts

def create_dataset(formatted_prompts, tokenizer):
    """Tokenizes the formatted prompts and creates a Hugging Face Dataset."""
    tokenized_inputs = tokenizer(
        formatted_prompts,
        max_length=MAX_SEQ_LENGTH,
        truncation=True,
        padding="max_length", # Pad to max_length for consistent batching. Consider 'longest' if memory is very tight and seq lengths vary greatly.
        return_tensors="pt" # Return PyTorch tensors
    )
    dataset = Dataset.from_dict(tokenized_inputs)
    return dataset

# --- Main Training Script ---
if __name__ == "__main__":
    # 1. Prepare Data
    print("Loading and formatting data...")
    formatted_train_prompts = read_training_data(DATA_FILE_PATH)

    if not formatted_train_prompts:
        print("Error: No valid data found in the input file. Please check the file format and content.")
        exit()

    print(f"Number of training records: {len(formatted_train_prompts)}")

    # 2. Load Model and Tokenizer
    print("Loading model and tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, use_fast=True, padding_side="right") # padding_side right is often preferred for causal LMs
    tokenizer.pad_token = tokenizer.eos_token # Important for Llama models if pad token isn't set
    model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16, device_map=device_map) # Use bfloat16 for faster and memory-efficient training if supported

    # 3. Prepare Dataset
    print("Creating dataset...")
    train_dataset = create_dataset(formatted_train_prompts, tokenizer)

    # --- 4a. Standard SFT Fine-tuning ---
    print("\n--- Starting Standard SFT Fine-tuning ---")
    sft_output_path = os.path.join(OUTPUT_DIR, SFT_OUTPUT_SUBDIR)
    training_args_sft = TrainingArguments(
        output_dir=sft_output_path,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRADIENT_ACCUMULATION_STEPS,
        learning_rate=LEARNING_RATE,
        num_train_epochs=NUM_EPOCHS,
        logging_steps=50, # Adjust logging frequency
        save_steps=500, # Adjust saving frequency
        save_total_limit=2, # Keep only the last 2 checkpoints
        push_to_hub=False, # Set to True and configure hub_model_id if you want to push to Hugging Face Hub
        fp16=True, # Use fp16 for faster and memory-efficient training if supported. Consider bf16 if your GPU supports it for potentially better stability.
        optim="paged_adamw_8bit", # Use efficient optimizer
        lr_scheduler_type="cosine", # Common scheduler
        warmup_ratio=0.03, # Warmup ratio
        weight_decay=0.01, # Weight decay for regularization
        dataloader_num_workers=2, # Adjust based on your CPU cores
        max_grad_norm=1.0, # Gradient clipping to prevent exploding gradients
    )

    trainer_sft = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=train_dataset,
        dataset_text_field=None, # Dataset is already tokenized
        max_seq_length=MAX_SEQ_LENGTH,
        args=training_args_sft,
        formatting_func=None # No formatting function needed as data is already formatted
    )

    trainer_sft.train()
    trainer_sft.save_model(sft_output_path)
    tokenizer.save_pretrained(sft_output_path) # Save tokenizer as well

    print(f"Standard SFT Fine-tuning finished. Model saved to {sft_output_path}")


    # --- 4b. QLoRA Fine-tuning ---
    print("\n--- Starting QLoRA Fine-tuning ---")
    qlora_output_path = os.path.join(OUTPUT_DIR, QLORA_OUTPUT_SUBDIR)

    # Load base model again for QLoRA (important to start from original weights for comparison)
    model_qlora = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16, device_map=device_map) # Reload base model

    lora_config = LoraConfig(
        r=LORA_RANK,
        lora_alpha=LORA_ALPHA,
        target_modules=TARGET_MODULES,
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type="CAUSAL_LM"
    )
    model_qlora = get_peft_model(model_qlora, lora_config)
    model_qlora.print_trainable_parameters() # Show trainable parameters

    training_args_qlora = TrainingArguments(
        output_dir=qlora_output_path,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRADIENT_ACCUMULATION_STEPS,
        learning_rate=LEARNING_RATE,
        num_train_epochs=NUM_EPOCHS,
        logging_steps=50,
        save_steps=500,
        save_total_limit=2,
        push_to_hub=False,
        fp16=True, # Or bf16 if supported
        optim="paged_adamw_8bit",
        lr_scheduler_type="cosine",
        warmup_ratio=0.03,
        weight_decay=0.01,
        dataloader_num_workers=2,
        max_grad_norm=1.0,
    )

    trainer_qlora = SFTTrainer(
        model=model_qlora,
        tokenizer=tokenizer,
        train_dataset=train_dataset,
        dataset_text_field=None,
        max_seq_length=MAX_SEQ_LENGTH,
        args=training_args_qlora,
        formatting_func=None
    )

    trainer_qlora.train()
    trainer_qlora.save_model(qlora_output_path) # Saves PEFT adapters
    tokenizer.save_pretrained(qlora_output_path)


    print(f"QLoRA Fine-tuning finished. Model (adapters) saved to {qlora_output_path}")

    print("\n--- Fine-tuning process completed! ---")
