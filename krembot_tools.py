import csv, json, re
import neo4j
import pandas as pd
import requests
import streamlit as st
import xml.etree.ElementTree as ET

from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain_community.vectorstores import Pinecone as LangPine
from langchain_openai import OpenAIEmbeddings
from langchain_openai.chat_models import ChatOpenAI

from openai import OpenAI
from os import getenv
from pinecone import Pinecone
from pinecone_text.sparse import BM25Encoder
from time import sleep
from typing import List, Dict

from krembot_db import work_prompts

mprompts = work_prompts()
client = OpenAI(api_key=getenv("OPENAI_API_KEY"))

@st.cache_resource
def connect_to_neo4j():
    return neo4j.GraphDatabase.driver(getenv("NEO4J_URI"), auth=(getenv("NEO4J_USER"), getenv("NEO4J_PASS")))


def connect_to_pinecone(x):
    pinecone_api_key = getenv('PINECONE_API_KEY')
    pinecone_host = "https://delfi-a9w1e6k.svc.aped-4627-b74a.pinecone.io" if x == 0 else "https://neo-positive-a9w1e6k.svc.apw5-4e34-81fa.pinecone.io"
    return Pinecone(api_key=pinecone_api_key, host=pinecone_host).Index(host=pinecone_host)


def rag_tool_answer(prompt):
    context = " "
    st.session_state.rag_tool = get_structured_decision_from_model(prompt)

    if  st.session_state.rag_tool == "Hybrid":
        processor = HybridQueryProcessor(namespace="delfi-podrska", delfi_special=1)
        context = processor.process_query_results(prompt)
        print(111, context)

    elif  st.session_state.rag_tool == "Opisi":
        uvod = mprompts["rag_self_query"]
        prompt = uvod + prompt
        context = SelfQueryDelfi(prompt)

    elif  st.session_state.rag_tool == "Korice":
        uvod = mprompts["rag_self_query"]
        prompt = uvod + prompt
        context = SelfQueryDelfi(upit=prompt, namespace="korice")
        
    elif  st.session_state.rag_tool == "Graphp": 
        context = graphp(prompt, False)

    elif st.session_state.rag_tool == "Pineg":
        context = pineg(prompt)

    elif st.session_state.rag_tool == "CSV":
        context = order_search(prompt)

    elif st.session_state.rag_tool == "Stolag":
        context = API_search(graphp(prompt, True) )

    elif st.session_state.rag_tool in ["InteliA", "InteliB", "InteliC", "InteliD", "InteliE"]:
        context = intelisale_csv(st.session_state.rag_tool, prompt)

    elif  st.session_state.rag_tool == "FAQ":
        processor = HybridQueryProcessor(namespace="ecd-faq", delfi_special=1)
        context = processor.process_query_results(prompt)
        # st.info("Score po chunku:")
        # st.write(scores)
        
    elif  st.session_state.rag_tool == "Uputstva":
        processor = HybridQueryProcessor(namespace="ecd-uputstva", delfi_special=1)
        context = processor.process_query_results(prompt)

    elif  st.session_state.rag_tool == "Blogovi":
        processor = HybridQueryProcessor(namespace="ecd-blogovi", delfi_special=1)
        context = processor.process_query_results(prompt)

    return context, st.session_state.rag_tool


def get_structured_decision_from_model(user_query):
    """
    Determines the most appropriate tool to use for a given user query using an AI model.

    This function sends a user query to an AI model and receives a structured decision in the
    form of a JSON object. The decision includes the recommended tool to use for addressing
    the user's query, based on the content and context of the query. The function uses a
    structured prompt, generated by `create_structured_prompt`, to instruct the AI on how
    to process the query. The AI's response is parsed to extract the tool recommendation.

    Parameters:
    - user_query: The user's query for which the tool recommendation is sought.

    Returns:
    - The name of the recommended tool as a string, based on the AI's analysis of the user query.
    """
    client = OpenAI()
    response = client.chat.completions.create(
        model=getenv("OPENAI_MODEL"),
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
        {"role": "system", "content": mprompts["choose_rag"]},
        {"role": "user", "content": f"Please provide the response in JSON format: {user_query}"}],
        )
    json_string = response.choices[0].message.content
    # Parse the JSON string into a Python dictionary
    data_dict = json.loads(json_string)
    # Access the 'tool' value
    return data_dict['tool'] if 'tool' in data_dict else list(data_dict.values())[0]


def graphp(pitanje, usingAPI):
    driver = connect_to_neo4j()
    namespace = 'opisi'
    def run_cypher_query(driver, query):
        with driver.session() as session:
            results = session.run(query)
            cleaned_results = []
            max_characters=100000
            total_characters = 0
            max_record_length = 0
            min_record_length = float('inf')
            
            for record in results:
                cleaned_record = {}
                for key, value in record.items():
                    if isinstance(value, neo4j.graph.Node):
                        # Ako je vrednost Node objekat, pristupamo properties atributima
                        properties = {k: v for k, v in value._properties.items()}
                    else:
                        # Ako je vrednost obična vrednost, samo je dodamo
                        properties = {key: value}
                    
                    for prop_key, prop_value in properties.items():
                        # Uklanjamo prefiks 'b.' ako postoji
                        new_key = prop_key.split('.')[-1]
                        cleaned_record[new_key] = prop_value
                
                record_length = sum(len(str(value)) for value in cleaned_record.values())
                if total_characters + record_length > max_characters:
                    break  # Prekida se ako dodavanje ovog zapisa prelazi maksimalan broj karaktera

                cleaned_results.append(cleaned_record)
                record_length = sum(len(str(value)) for value in cleaned_record.values())
                total_characters += record_length
                if record_length > max_record_length:
                    max_record_length = record_length
                if record_length < min_record_length:
                    min_record_length = record_length

        return cleaned_results
        
    def generate_cypher_query(question):
        prompt = f"Translate the following user question into a Cypher query. Use the given structure of the database: {question}"
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.0,
            messages=[
                {
            "role": "system",
            "content": (
                "You are a helpful assistant that converts natural language questions into Cypher queries for a Neo4j database."
                "The database has 3 node types: Author, Book, Genre, and 2 relationship types: BELONGS_TO and WROTE."
                "Only Book nodes have properties: id, oldProductId, category, title, price, quantity, pages, and eBook."
                "All node and relationship names are capitalized (e.g., Author, Book, Genre, BELONGS_TO, WROTE)."
                "Genre names are also capitalized (e.g., Drama, Fantastika). Please ensure that the generated Cypher query uses these exact capitalizations."
                "Ensure to include a condition to check that the quantity property of Book nodes is greater than 0 to ensure the books are in stock where this filter is plausable."
                "For recommendations, ensure to return just the title and the author of the recommended books."
                "When writing the Cypher query, ensure that instead of '=' use CONTAINS, in order to return all items which contains the seatched term."
                "When generating the Cypher query, ensure to handle inflected forms properly. For example, if the user asks for books by 'Tolkiena,' generate a query for 'Tolkien' instead, removing any inflections."
                "When returning some properties of books, ensure to return the title too."
                "Ensure that the Cypher query returns only the requested data. If the user does not specify which properties they want to retrieve, return only the title and the author."

                "Here is an example user question and the corresponding Cypher query: "
                "Example user question: 'Pronađi knjigu Da Vinčijev kod.' "
                "Cypher query: MATCH (b:Book) WHERE toLower(b.title) CONTAINS toLower('Da Vinčijev kod') RETURN b"

                "Example user question: 'Interesuje me knjiga Piramide.' "
                "Cypher query: MATCH (b:Book)-[:WROTE]-(a:Author) WHERE toLower(b.title) CONTAINS toLower('Piramide') AND b.quantity > 0 RETURN b.title AS title, b.oldProductId AS oldProductId, b.category AS category, a.name AS author"
                
                "Example user question: 'Preporuci mi knjige slicne knjizi Krhotine.' "
                "Cypher query: MATCH (b:Book)-[:BELONGS_TO]->(g:Genre) WHERE toLower(b.title) CONTAINS toLower('Krhotine') WITH g MATCH (rec:Book)-[:BELONGS_TO]->(g)<-[:BELONGS_TO]-(b:Book) WHERE b.title CONTAINS 'Krhotine' AND rec.quantity > 0 MATCH (rec)-[:WROTE]-(a:Author) RETURN rec.title AS title, rec.oldProductId AS oldProductId, b.category AS category, a.name AS author"

                "Example user question: 'Koja je cena za Autostoperski vodič kroz galaksiju?' "
                "Cypher query: MATCH (b:Book) WHERE toLower(b.title) CONTAINS toLower('Autostoperski vodič kroz galaksiju') AND b.quantity > 0 RETURN b.title AS title, b.oldProductId AS oldProductId, b.category AS category, b.price AS price"

                "Example user question: 'Da li imate anu karenjinu na stanju' "
                "Cypher query: MATCH (b:Book) WHERE toLower(b.title) CONTAINS toLower('Ana Karenjina') AND b.quantity > 0 RETURN b.title AS title, b.oldProductId AS oldProductId, b.category AS category"

                "Example user question: 'Da li imate mobi dik na stanju, treba mi 27 komada?' "
                "Cypher query: MATCH (b:Book) WHERE toLower(b.title) CONTAINS toLower('Mobi Dik') AND b.quantity > 27 RETURN b.title AS title, b.quantity AS quantity, b.oldProductId AS oldProductId, b.category AS category"

                "ALWAYS return oldProductId ID, regardless of the user's question."
            )
        },
                {"role": "user", "content": prompt}
            ]
        )
        cypher_query = response.choices[0].message.content.strip()

        if '```cypher' in cypher_query:
            cypher_query = cypher_query.split('```cypher')[1].split('```')[0].strip()
        
        if cypher_query.endswith('.'):
            cypher_query = cypher_query[:-1].strip()

        return cypher_query

    def create_product_links(products):
        static_url = 'https://delfi.rs/'
        updated_products = []
        
        for product in products:
            if 'oldProductId' in product and 'category' in product:
                product['link'] = static_url + product['category'].lower().replace(' ', '_') + '/' + str(product.pop('oldProductId'))
            updated_products.append(product)
        
        return updated_products


    def get_descriptions_from_pinecone(ids):
        index = connect_to_pinecone(x=0)

        results = index.fetch(ids=ids, namespace=namespace)
        descriptions = {}

        for id in ids:
            if id in results['vectors']:
                vector_data = results['vectors'][id]
                if 'metadata' in vector_data:
                    descriptions[id] = vector_data['metadata'].get('text', 'No description available')
                else:
                    descriptions[id] = 'Metadata not found in vector data.'
            else:
                descriptions[id] = 'Nemamo opis za ovaj artikal.'
        return descriptions

    def combine_data(book_data, descriptions):
        combined_data = []

        for book in book_data:        
            print(f"Book: {book}")
            book_id = book.get('id', None)
            
            print(f"Book ID: {book_id}")
            description = descriptions.get(book_id, 'No description available')
            combined_entry = {**book, 'description': description}
            combined_data.append(combined_entry)
        
        return combined_data

    def display_results(combined_data):
        output = " "
        for data in combined_data:
            if 'title' in data:
                output += f"Title: {data['title']}\n"
            if 'category' in data:
                output += f"Category: {data['category']}\n"
            if 'price' in data:
                output += f"Price: {data['price']}\n"
            if 'quantity' in data:
                output += f"Quantity: {data['quantity']}\n"
            if 'pages' in data:
                output += f"Pages: {data['pages']}\n"
            if 'eBook' in data:
                output += f"eBook: {data['eBook']}\n"
            if 'description' in data:
                output += f"Description: {data['description']}\n"
            if 'link' in data:
                output += f"Link: {data['link']}\n"

    def is_valid_cypher(cypher_query):
        # Provera validnosti Cypher upita (osnovna provera)
        if not cypher_query or "MATCH" not in cypher_query.upper():
            # print("Cypher upit nije validan.")
            return False
        # print("Cypher upit je validan.")
        return True

    def formulate_answer_with_llm(question, graph_data):
        input_text = f"Pitanje: '{question}'\nPodaci iz grafa: {graph_data}\nMolimo formulišite odgovor na osnovu ovih podataka."
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.0,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that formulates answers based on given data. You have been provided with a user question and data returned from a graph database. Please formulate an answer based on these inputs."},
                {"role": "user", "content": input_text}
            ]
        )
        return response.choices[0].message.content.strip()

    cypher_query = generate_cypher_query(pitanje)
    
    if is_valid_cypher(cypher_query):
        try:
            cleaned_data = run_cypher_query(driver, cypher_query)
            book_data = create_product_links(cleaned_data)
            pattern = r"'(?:b\.)?id': '([^']+)'"

            if usingAPI:
                return [re.search(r'/(\d+)$', item['link']).group(1) for item in book_data]

            book_ids = []
            try:
                for data in book_data:
                    match = re.search(pattern, str(data))
                    if match:
                        book_ids.append(match.group(1))
            except Exception as e:
                print(f"An error occurred: {e}")
            
            if not book_ids:
                print("Vraćeni podaci ne sadrže 'id' polje.")
                return formulate_answer_with_llm(pitanje, book_data)
            
            descriptionsDict = get_descriptions_from_pinecone(book_ids)

            return display_results(combine_data(book_data, descriptionsDict))
        except Exception as e:
            print(f"Greška pri izvršavanju upita: {e}. Molimo pokušajte ponovo.")


def pineg(pitanje):
    namespace = 'opisi'
    index = connect_to_pinecone(x=0)
    driver = connect_to_neo4j()

    def run_cypher_query(id):
        query = f"MATCH (b:Book) WHERE b.id = '{id}' RETURN b"
        with driver.session() as session:
            result = session.run(query)
            book_data = []
            for record in result:
                book_node = record['b']
                book_data.append({
                    'id': book_node['id'],
                    'oldProductId': book_node['oldProductId'],
                    'title': book_node['title'],
                    'category': book_node['category'],
                    'price': book_node['price'],
                    'quantity': book_node['quantity'],
                    'pages': book_node['pages'],
                    'eBook': book_node['eBook']
                })
            return book_data

    def get_embedding(text, model="text-embedding-3-large"):
        response = client.embeddings.create(
            input=[text],
            model=model
        ).data[0].embedding
        
        return response

    def dense_query(query, top_k=5, filter=None, namespace=namespace):
        dense = get_embedding(text=query)

        query_params = {
            'top_k': top_k,
            'vector': dense,
            'include_metadata': True,
            'namespace': namespace
        }

        response = index.query(**query_params)
        matches = response.to_dict().get('matches', [])

        return matches

    def search_pinecone(query: str, top_k: int = 5) -> List[Dict]:
        query_embedding = dense_query(query)
        matches = []
        for match in query_embedding:
            metadata = match['metadata']
            matches.append({
                'id': metadata['id'],
                'text': metadata['text']
            })
        
        # print(f"Matches: {matches}")
        return matches

    def create_product_links(products):
        static_url = 'https://delfi.rs/'
        updated_products = []
        
        for product in products:
            if 'oldProductId' in product and 'category' in product:
                product['link'] = static_url + product['category'].lower().replace(' ', '_') + '/' + str(product.pop('oldProductId'))
            updated_products.append(product)
        
        return updated_products

    def combine_data(book_data, descriptions):
        combined_data = []
        for book in book_data:
            combined_entry = {**book, 'description': descriptions}
            combined_data.append(combined_entry)
            print(f"Combined Entry: {combined_entry}")
        return combined_data

    def display_results(combined_data):
        output = " "
        for data in combined_data:
            output += f"Title: {data['title']}\n"
            output += f"Category: {data['category']}\n"
            output += f"Price: {data['price']}\n"
            output += f"Quantity: {data['quantity']}\n"
            output += f"Pages: {data['pages']}\n"
            output += f"eBook: {data['eBook']}\n"
            output += f"Description: {data['description']}\n"
            output += f"Link: {data['link']}\n"
        return output

    search_results = search_pinecone(pitanje)

    combined_results = []

    for result in search_results:
        
        try:
            data = run_cypher_query(result['id'])
        except:
            sleep(0.1)
            data = run_cypher_query(result['id'])
        additional_data = create_product_links(data)
        print(f"Additional Data: {additional_data}")
        
        combined_data = combine_data(additional_data, result['text'])
        combined_results.append(combined_data)

    return display_results(combined_data)


def order_search(id_porudzbine):
    match = re.search(r'\d{5,}', id_porudzbine)
    if not match:
        return "No integer found in the prompt."
    
    order_number = int(match.group())

    try:
        with open('orders.csv', mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                if int(row[0]) == order_number:
                    return ", ".join(row)
        return f"Order number {order_number} not found in the CSV file."
    except FileNotFoundError:
        return "The file 'orders.csv' does not exist."
    except Exception as e:
        return f"An error occurred: {e}"
    

def API_search(matching_sec_ids):

    def get_product_info(token, product_id):
        return requests.get(url="https://www.delfi.rs/api/products", params={"token": token, "product_id": product_id}).content

    # Function to parse the XML response and extract required fields
    def parse_product_info(xml_data):
        product_info = {}
        try:
            root = ET.fromstring(xml_data)
            product_node = root.find(".//product")
            if product_node is not None:
                cena = product_node.findtext('cena')
                lager = product_node.findtext('lager')
                url = product_node.findtext('url')
                
                if lager and int(lager) > 2:
                    product_info = {
                        'cena': cena,
                        'lager': lager,
                        'url': url
                    }
                else:
                    print(f"Skipping product with lager {lager}")  # Debugging line
            else:
                print("Product node not found in XML data")  # Debugging line
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")  # Debugging line
        return product_info

    # Main function to get info for a list of product IDs
    def get_multiple_products_info(token, product_ids):
        products_info = []
        for product_id in product_ids:
            xml_data = get_product_info(token, product_id)
            print(f"XML data for product_id {product_id}: {xml_data}")  # Debugging line
            product_info = parse_product_info(xml_data)
            if product_info:  # Only add if product info is found and lager > 2
                products_info.append(product_info)
        return products_info

    # Replace with your actual token and product IDs
    token = getenv("DELFI_API_KEY")
    product_ids = matching_sec_ids

    try:
        products_info = get_multiple_products_info(token, product_ids)
    except:
        products_info = "No products found for the given IDs."
    print(f"Products Info: {products_info}")
    output = "Data returned from API for each searched id: \n"
    for info in products_info:
        output += str(info) + "\n"
    return output


def SelfQueryDelfi(upit, api_key=None, environment=None, index_name='delfi', namespace='opisi', openai_api_key=None, host=None):
    """
    Executes a query against a Pinecone vector database using specified parameters or environment variables. 
    The function initializes the Pinecone and OpenAI services, sets up the vector store and metadata, 
    and performs a query using a custom retriever based on the provided input 'upit'.

    It is used for self-query on metadata.

    Parameters:
    upit (str): The query input for retrieving relevant documents.
    api_key (str, optional): API key for Pinecone. Defaults to PINECONE_API_KEY from environment variables.
    environment (str, optional): Pinecone environment. Defaults to PINECONE_API_KEY from environment variables.
    index_name (str, optional): Name of the Pinecone index to use. Defaults to 'positive'.
    namespace (str, optional): Namespace for Pinecone index. Defaults to NAMESPACE from environment variables.
    openai_api_key (str, optional): OpenAI API key. Defaults to OPENAI_API_KEY from environment variables.

    Returns:
    str: A string containing the concatenated results from the query, with each document's metadata and content.
         In case of an exception, it returns the exception message.

    Note:
    The function is tailored to a specific use case involving Pinecone and OpenAI services. 
    It requires proper setup of these services and relevant environment variables.
    """
    
    # Use the passed values if available, otherwise default to environment variables
    api_key = api_key if api_key is not None else getenv('PINECONE_API_KEY')
    environment = environment if environment is not None else getenv('PINECONE_API_KEY')
    # index_name is already defaulted to 'positive'
    namespace = namespace if namespace is not None else getenv("NAMESPACE")
    openai_api_key = openai_api_key if openai_api_key is not None else getenv("OPENAI_API_KEY")
    host = host if host is not None else getenv("PINECONE_HOST")
   
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

    # prilagoditi stvanim potrebama metadata
    metadata_field_info = [
        AttributeInfo(name="authors", description="The author(s) of the document", type="string"),
        AttributeInfo(name="category", description="The category of the document", type="string"),
        AttributeInfo(name="chunk", description="The chunk number of the document", type="integer"),
        AttributeInfo(name="date", description="The date of the document", type="string"),
        AttributeInfo(name="eBook", description="Whether the document is an eBook", type="boolean"),
        AttributeInfo(name="genres", description="The genres of the document", type="string"),
        AttributeInfo(name="id", description="The unique ID of the document", type="string"),
        AttributeInfo(name="text", description="The main content of the document", type="string"),
        AttributeInfo(name="title", description="The title of the document", type="string"),
        AttributeInfo(name="sec_id", description="The ID for the url generation", type="string"),
    ]

    # Define document content description
    document_content_description = "Content of the document"

    # Prilagoditi stvanom nazivu namespace-a
    text_key = "text" if namespace == "opisi" else "description"
    vectorstore = LangPine.from_existing_index(
        index_name=index_name, embedding=embeddings, text_key=text_key, namespace=namespace)

    # Initialize OpenAI embeddings and LLM
    llm = ChatOpenAI(model="gpt-4o", temperature=0.0)
    retriever = SelfQueryRetriever.from_llm(
        llm,
        vectorstore,
        document_content_description,
        metadata_field_info,
        enable_limit=True,
        verbose=True,
    )
    try:
        result = ""
        doc_result = retriever.get_relevant_documents(upit)
        for doc in doc_result:
            metadata = doc.metadata
            result += (
                f"Sec_id: {str(metadata['sec_id'])}\n"
                f"Title: {str(metadata['title'])}\n"
                f"Authors: {', '.join(map(str, metadata['authors']))}\n"
                f"Chunk: {str(metadata['chunk'])}\n"
                f"Date: {str(metadata['date'])}\n"
                f"eBook: {str(metadata['eBook'])}\n"
                f"Genres: {', '.join(map(str, metadata['genres']))}\n"
                f"URL: https://delfi.rs/{str(metadata['category'])}/{str(metadata['sec_id'])}\n"
                f"ID: {str(metadata['id'])}\n"
                f"Content: {str(doc.page_content)}\n\n"
            )
        print(result)
        return result.strip()

    except Exception as e:
        print(e)
        return str(e)


class HybridQueryProcessor:
    """
    A processor for executing hybrid queries using Pinecone.

    This class allows the execution of queries that combine dense and sparse vector searches,
    typically used for retrieving and ranking information based on text data.

    Attributes:
        api_key (str): The API key for Pinecone.
        environment (str): The Pinecone environment setting.
        alpha (float): The weight used to balance dense and sparse vector scores.
        score (float): The score treshold.
        index_name (str): The name of the Pinecone index to be used.
        index: The Pinecone index object.
        namespace (str): The namespace to be used for the Pinecone index.
        top_k (int): The number of results to be returned.
            
    Example usage:
    processor = HybridQueryProcessor(api_key=environ["PINECONE_API_KEY"], 
                                 environment=environ["PINECONE_API_KEY"],
                                 alpha=0.7, 
                                 score=0.35,
                                 index_name='custom_index'), 
                                 namespace=environ["NAMESPACE"],
                                 top_k = 10 # all params are optional

    result = processor.hybrid_query("some query text")    
    """

    def __init__(self, **kwargs):
        """
        Initializes the HybridQueryProcessor with optional parameters.

        The API key and environment settings are fetched from the environment variables.
        Optional parameters can be passed to override these settings.

        Args:
            **kwargs: Optional keyword arguments:
                - api_key (str): The API key for Pinecone (default fetched from environment variable).
                - environment (str): The Pinecone environment setting (default fetched from environment variable).
                - alpha (float): Weight for balancing dense and sparse scores (default 0.5).
                - score (float): Weight for balancing dense and sparse scores (default 0.05).
                - index_name (str): Name of the Pinecone index to be used (default 'positive').
                - namespace (str): The namespace to be used for the Pinecone index (default fetched from environment variable).
                - top_k (int): The number of results to be returned (default 6).
        """
        self.api_key = kwargs.get('api_key', getenv('PINECONE_API_KEY'))
        self.environment = kwargs.get('environment', getenv('PINECONE_API_KEY'))
        self.alpha = kwargs.get('alpha', 0.5)  # Default alpha is 0.5
        self.score = kwargs.get('score', 0.05)  # Default score is 0.05
        self.index_name = kwargs.get('index', 'neo-positive')  # Default index is 'positive'
        self.namespace = kwargs.get('namespace', getenv("NAMESPACE"))  
        self.top_k = kwargs.get('top_k', 6)  # Default top_k is 6
        self.delfi_special = kwargs.get('delfi_special')
        self.index = connect_to_pinecone(self.delfi_special)
        self.host = getenv("PINECONE_HOST")

    def hybrid_score_norm(self, dense, sparse):
        """
        Normalizes the scores from dense and sparse vectors using the alpha value.

        Args:
            dense (list): The dense vector scores.
            sparse (dict): The sparse vector scores.

        Returns:
            tuple: Normalized dense and sparse vector scores.
        """
        return ([v * self.alpha for v in dense], 
                {"indices": sparse["indices"], 
                 "values": [v * (1 - self.alpha) for v in sparse["values"]]})
    
    def hybrid_query(self, upit, top_k=None, filter=None, namespace=None):
        # Get embedding and unpack results
        dense = self.get_embedding(text=upit)

        # Use those results in another function call
        hdense, hsparse = self.hybrid_score_norm(
            sparse=BM25Encoder().fit([upit]).encode_queries(upit),
            dense=dense
        )

        query_params = {
            'top_k': top_k or self.top_k,
            'vector': hdense,
            'sparse_vector': hsparse,
            'include_metadata': True,
            'namespace': namespace or self.namespace
        }

        if filter:
            query_params['filter'] = filter

        response = self.index.query(**query_params)

        matches = response.to_dict().get('matches', [])
        results = []

        for match in matches:
            try:
                metadata = match.get('metadata', {})

                # Create the result entry with all metadata fields
                result_entry = metadata.copy()

                # Ensure mandatory fields exist with default values if they are not in metadata
                result_entry.setdefault('context', '')
                result_entry.setdefault('chunk', None)
                result_entry.setdefault('source', None)
                result_entry.setdefault('score', match.get('score', 0))

                # Only add to results if 'context' exists
                if result_entry['context']:
                    results.append(result_entry)
            except Exception as e:
                # Log or handle the exception if needed
                print(f"An error occurred: {e}")
                pass

        return results
       
    def process_query_results(self, upit, dict=False):
        """
        Processes the query results and prompt tokens based on relevance score and formats them for a chat or dialogue system.
        Additionally, returns a list of scores for items that meet the score threshold.
        """
        tematika = self.hybrid_query(upit)
        print(222, tematika)
        if not dict:
            uk_teme = ""
            
            for item in tematika:
                if item["score"] > self.score:
                    # Build the metadata string from all relevant fields
                    metadata_str = "\n".join(f"{key}: {value}" for key, value in item.items())
                    # Append the formatted metadata string to uk_teme
                    uk_teme += metadata_str + "\n\n"
            
            return uk_teme
        else:
            return tematika
        
    def get_embedding(self, text, model="text-embedding-3-large"):

        """
        Retrieves the embedding for the given text using the specified model.

        Args:
            text (str): The text to be embedded.
            model (str): The model to be used for embedding. Default is "text-embedding-3-large".

        Returns:
            list: The embedding vector of the given text.
            int: The number of prompt tokens used.
        """
        
        text = text.replace("\n", " ")
        result = client.embeddings.create(input=[text], model=model).data[0].embedding
       
        return result
    

def intelisale_csv(query_type, cid):
    match = re.search(r'\d{1,}', cid)
    if not match:
        return "No integer found in the prompt."
    
    cid = int(match.group())
    # Get the list of CSV files in the current directory
    # ["Intelisale_Activities.csv", "Intelisale_Attributes.csv", "Intelisale_Customers.csv", "Intelisale_Notes.csv", "Intelisale_PGP.csv"]
    customers_df = pd.read_csv("Intelisale_Customers.csv")
    pgp_df = pd.read_csv("Intelisale_PGP.csv")
    notes_df = pd.read_csv("Intelisale_Notes.csv")
    

    if query_type == "InteliA":
        filtered_data = customers_df[customers_df['CustomerId'] == cid]
        results = filtered_data[['CustomerId', 'Code', 'TopDivision', 'Division', 'TopBranch', 'Branch', 'BlueCoatsNo']]

    elif query_type == "InteliB":
        customer_plan_data = customers_df[['CustomerId', 'PlanCurrentYear', 'TurnoverCurrentYear', 'FullfilmentCurrentYear', 'Plan12Months', 'Turnover12Months', 'Fullfilment12Months']]
        product_potential_data = pgp_df[['Turnover', 'Potential', 'UnusedPotential']]
        
        filtered_customer_plan_data = customer_plan_data[customer_plan_data['CustomerId'] == cid]
        filtered_product_potential_data = product_potential_data[pgp_df['CustomerId'] == cid]  # Adjust if needed
        
        results = {
            'customer_plan_data': filtered_customer_plan_data,
            'product_potential_data': filtered_product_potential_data
        }

    elif query_type == "InteliC":
        filtered_data = customers_df[customers_df['CustomerId'] == cid]
        results = filtered_data[['CustomerId', 'Turnover12Months', 'CalculatedNumberOfVisits', 'CalculatedTimeAtCustomer']]

    elif query_type == "InteliD":
        filtered_data = customers_df[customers_df['CustomerId'] == cid]
        results = filtered_data[['CustomerId', 'CreditLimit', 'Balance', 'BalanceOutOfLimit', 'BalanceCritical']]

    elif query_type == "InteliE":
        filtered_data_by_customer_id = notes_df[notes_df['CustomerId'] == cid]
        if filtered_data_by_customer_id.empty:
            filtered_data_by_id = notes_df[notes_df['Id'] == cid]
            results = filtered_data_by_id[['Id', 'NoteContent', 'CustomerId']]
        else:
            results = filtered_data_by_customer_id[['Id', 'NoteContent', 'CustomerId']]

    return results.to_string()
