
import gradio as gr
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate

# Initialize the LLM (replace with your API key)
llm = ChatGroq(
    temperature=0,
    groq_api_key='YOUR_API_KEY',  # Replace with your Groq API Key
    model_name="llama-3.1-70b-versatile"
)

# Define the email generation logic
def generate_email(subject, tone, additional_details):
    prompt_template = PromptTemplate.from_template(
        """
        ### EMAIL GENERATION PROMPT:
        Subject: {subject}
        Tone: {tone}
        Additional Details: {additional_details}
        Your job is to generate a professional email based on the above details. 
        Make it concise and appropriate.
        ### GENERATED EMAIL:
        """
    )
    chain = prompt_template | llm
    response = chain.invoke(input={
        'subject': subject,
        'tone': tone,
        'additional_details': additional_details
    })
    return response.content

# Define the Gradio interface
with gr.Blocks() as demo:
    gr.Markdown("## LLM-Based Email Generator")
    
    with gr.Row():
        subject = gr.Textbox(label="Email Subject", placeholder="Enter the subject of the email")
        tone = gr.Dropdown(["Formal", "Informal", "Neutral"], label="Tone", value="Formal")
    
    additional_details = gr.Textbox(
        label="Additional Details", placeholder="Provide extra details (optional)", lines=4
    )
    
    output = gr.Textbox(label="Generated Email", lines=10)
    
    generate_btn = gr.Button("Generate Email")
    generate_btn.click(
        fn=generate_email,
        inputs=[subject, tone, additional_details],
        outputs=[output]
    )

# Launch the app
demo.launch()
