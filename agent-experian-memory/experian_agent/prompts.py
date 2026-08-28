"""Prompts carried over from Experian's access-bot, plus memory-extraction.

EXPERIAN_PRODUCTS doubles as the static knowledge base for the retrieval step:
staging has no access to Experian's vector search index, so retrieve_context()
keyword-scores these product bullets instead. The pipeline shape (rewrite →
retrieve → generate) is unchanged.
"""

EXPERIAN_PRODUCTS = """• Bill Negotiation & Non-Experian Subscription Cancellation: Experian BillFixer helps users lower their bills and cancel unwanted subscriptions to save money and manage expenses effectively.
• Account Management & Info Changes: Experian allows users to manage and update their personal information, such as name, address, and phone number, directly through their account, ensuring accurate and up-to-date credit reports.
• Marketplace: Experian's Marketplace provides a variety of credit card, personal loan and auto insurance options for users. It does this by leveraging a user's credit profile to match them with curated offers, helping them to compare options and find their best fit. Partnering with various third-party credit card issuers—including national, regional, community, fintech, online, subprime, private lenders—it helps consumers with offers that match their profiles.
• Credit, Scores & Reports: Experian provides access to credit reports and scores, offering tools to monitor changes and understand credit health. Users can view their FICO® Scores and receive personalized insights to manage their credit effectively.
• Freeze/Security Lock: Experian offers a credit freeze service that restricts access to your credit report, protecting against identity theft. This can be managed in real time through their online platform.
• Fraud & Security: Experian's fraud and security services include monitoring for identity theft, alerts for suspicious activity, and resources for resolving fraud-related issues, ensuring comprehensive protection for your personal information.
• Customer Service & Support: Experian provides robust customer support through various channels to assist with service-related queries and issues, ensuring customer satisfaction and effective resolution of problems.
• Disputes & Corrections: Users can dispute inaccuracies on their credit reports directly through Experian, which facilitates the correction process to maintain accurate and fair credit information.
• Adding & Managing Accounts/Connected Accounts: Experian allows users to add and manage linked accounts, providing a consolidated view of financial activities to help track and manage their credit health. To better help users manage their finances, Experian can also identify recurring payments & deposits and find subscriptions, while displaying the complete transaction history all in one place. Users may also be eligible for Experian Boost®, which can potentially improve their credit profile using transactions from their connected accounts.
• Loan & Mortgage Queries: Experian offers guidance on loan and mortgage options, helping users make informed decisions about borrowing and financing. This includes comparisons and recommendations tailored to individual needs.
• Payment & Billing Issues: Experian addresses payment and billing concerns related to their services, ensuring that users can resolve any discrepancies or issues with account charges smoothly.
• Alerts, Email & Communication Preferences: Users can customize their notification settings to receive alerts about changes in their credit report and other Experian services, ensuring they stay informed and can take timely action.
• Auto Loans: Experian offers information and support for obtaining and managing auto loans, including tools for comparing different loan options to find the best fit for the user's needs.
• Insurance: Experian offers a comprehensive insurance comparison tool that allows users to compare auto and home insurance quotes from over 40 top insurers. This service helps consumers find the best rates and coverage options, potentially saving them over $900 annually on auto insurance alone. The tool includes options for various coverage types such as comprehensive, collision, medical payments, and personal injury protection. It is free to use and provides personalized quotes based on current policy details, simplifying the process of switching to a better policy. Experian also supports users through the sign-up process and ensures seamless policy transitions.
• Experian Membership Cancellation: Experian provides users with assistance for canceling their Experian membership and requesting refunds.
• Experian Boost®: This free service allows users to add utility, telecom, insurance payments and more to their credit report, potentially boosting their credit score instantly by including positive payment history. Auto-add is a feature of Experian Boost and when toggled on any eligible bill that positively impacts your FICO® Score is automatically added to your Experian credit file.
• Experian Smart Money™: Experian Smart Money™ is a digital checking account and debit card that integrates with Experian Boost® to help users build credit by getting credit for eligible bill payments. It offers features such as no monthly fees, early paycheck access, and access to over 55,000 fee-free ATMs.
• Chatbot: Experian's personalized credit education and financial assistant chatbot EVA. This is you, the chatbot the user is speaking with right now. Any questions regarding EVA itself or addressing "you" specifically should list this product in the intent."""

QUERY_REWRITE_PROMPT = (
    "Decompose the user request into self-contained search queries:\n"
    "- Consider the *entire* conversation history for context to determine the user intent.\n"
    "- Generate 1-2 sub-queries for simple requests; 3-5 for complex ones.\n"
)

ANSWER_GENERATION_PROMPT = (
    "As EVA (Experian Virtual Assistant), the human-like personalized AI chatbot assistant built by Experian, "
    "you serve web and mobile customers. "
    "Your task is to generate a grounded answer using the provided context and instructions below:\n"
    "- Reason over the context, which describes Experian products relevant to the query.\n"
    "\t- If context is insufficient, say so; prefer prioritized_context over other_context.\n"
    "\t- Do not mention our documents by name in your answer; those are for your internal use "
    "only and should not be referenced in the user-facing response.\n"
    "- Reason over long_term_memory — durable facts remembered about this user from prior "
    "conversations — and personalize the answer with them where relevant. If a remembered fact "
    "directly informs your answer, use it naturally (do not recite the memory list back).\n"
    "- Consider the conversation history so follow-up questions resolve correctly."
)

MEMORY_EXTRACTION_PROMPT = (
    "You maintain the long-term memory of a customer-support assistant. From this completed "
    "conversation turn, extract durable facts about the user worth remembering across future "
    "conversations (preferences, goals, life events, products they use, constraints).\n"
    "- Each memory must be a single self-contained sentence about the user.\n"
    "- Do NOT extract transient details, questions, or anything about the assistant.\n"
    "- Return an empty list when the turn reveals nothing durable — most turns don't."
)
