import ollama
from Ollama.Resume_analysis.prompts import PROMPT


def analyze_resume(resume_text):
    prompt = PROMPT.format(resume=resume_text)

    response = ollama.chat(
        model="qwen2.5:3b",
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return response["message"]["content"]


if __name__ == "__main__":
    sample_resume = """
    John Doe

    Skills:
    - Python
    - Machine Learning
    - SQL
    - Docker

    Experience:
    Data Scientist with 3 years of experience building AI models.
    """

    result = analyze_resume(sample_resume)
    print(result)