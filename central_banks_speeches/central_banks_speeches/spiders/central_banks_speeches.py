import scrapy
import re
import io
import geonamescache
from nltk.tokenize import word_tokenize
from geotext import GeoText
import urllib
import gender_guesser.detector as gender

from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine


class CentralBanksSpeechesScraper(scrapy.Spider):
    """"""

    name = "central_banks_speeches"
    bank_pattern = re.compile('.*(by|at|of)\sthe\s(?P<bank_name>.*)')


    def generate_pages_urls(self, date_pages):
        """"""

        pages_urls = []
        for i in range(int(date_pages[1])):
            pages_urls.append(
                "https://www.bis.org/list/cbspeeches/from_{}/page_{}.htm".format(
                    date_pages[0], str(i+1)))
        return pages_urls


    def start_requests(self):
        """"""

        with open("../date_ranges.txt") as f:
            lines = f.readlines()
            dates_pages_mapping = [
                (line.split(",")[0].strip(), line.split(",")[1].strip())
                for line in lines]
        all_dates_pages_urls = []
        for date_pages in dates_pages_mapping:
            all_dates_pages_urls += self.generate_pages_urls(date_pages)
        for url in all_dates_pages_urls:
            yield scrapy.Request(
                url = url, callback = self.scrape_date_page)


    def scrape_date_page(self, response):
        """"""

        documents_container = response.css("#documents")[0]
        items = documents_container.css(".item")
        for item in items:
            speech_url = "https://www.bis.org/" + item.css("a::attr(href)").extract_first()
            if speech_url.endswith(".htm"):
                yield scrapy.Request(
                    url = speech_url, callback = self.scrape_speech_as_htm)
            else:
                pdf_speech_dict = self.scrape_speech_as_pdf(item, speech_url)
                yield pdf_speech_dict


    def scrape_speech_speaker(self, speech_dict, speaker_info):
        """"""

        if len(speaker_info)<=1:
            speaker_info = speaker_info[0].split(",")
            speech_dict["speaker_name"] = speaker_info[0].split("by")[-1].strip()
            speaker_gender = self.get_speaker_gender(
                speaker_info[0], speech_dict["speaker_name"])
            if speaker_gender is not None:
                speech_dict["speaker_gender"] = speaker_gender
            speech_dict["upload_date"] = speaker_info[-1].replace(".", "").strip()
            title_location_phrase = ",".join(speaker_info[1:-1]).strip()
        else:
            speech_dict["speaker_name"] = speaker_info[1]
            speaker_gender_phrase = speaker_info[0].strip().split(" ")[-1]
            speaker_gender = self.get_speaker_gender(
                speaker_gender_phrase, speech_dict["speaker_name"])
            if speaker_gender is not None:
                speech_dict["speaker_gender"] = speaker_gender
            speech_dict["upload_date"] = speaker_info[-1].split(",")[-1].replace(".", "").strip()
            title_location_phrase = ",".join(speaker_info[-1].strip().split(",")[1:-1]).strip()
    
        speaker_title = self.get_speaker_title(title_location_phrase)
        if speaker_title is not None:
            speech_dict["speaker_title"] = speaker_title
        central_bank_name = self.get_central_bank_name(title_location_phrase)
        if central_bank_name is not None:
            speech_dict["central_bank_name"] = central_bank_name
        speech_city, speech_country = self.get_speech_location(title_location_phrase)
        if speech_city is not None:
            speech_dict["speech_city"] = speech_city
        if speech_country is not None:
            speech_dict["speech_country"] = speech_country
        return


    def scrape_speech_as_pdf(self, item, speech_url):
        """"""

        speech_dict = {}
        speech_dict["title"] = item.css(
            ".item_date::attr(title)").extract_first().strip()
        speech_dict["date"] = item.css(".item_date::text").extract_first().strip()
        speaker_info = item.css(".info *::text").extract()
        speaker_info = [re.sub("(\r|\n|\t)+", "", item).strip() for item in speaker_info]
        speaker_info = [item for item in speaker_info if item!=""]
        self.scrape_speech_speaker(speech_dict, speaker_info)
        pdf_text = self.get_text_from_pdf(speech_url)
        if pdf_text is not None:
            speech_dict["pdf_text"] = pdf_text
        return speech_dict


    def scrape_speech_as_htm(self, response):
        """"""

        speech_dict = {}
        speech_center = response.css("#center")[0]
        speech_dict["title"] = speech_center.css("h1::text").extract_first()
        speech_dict["date"] = response.css(".date::text").extract_first().strip()
        speaker_info = speech_center.css("#extratitle-div  *::text").extract()
        speaker_info = [re.sub("(\r|\n|\t)+", "", item).strip() for item in speaker_info]
        speaker_info = [item for item in speaker_info if item!=""]
        self.scrape_speech_speaker(speech_dict, speaker_info)
        text_paragraphs = response.css("#cmsContent *::text").extract()
        text_paragraphs = [
            item for item in text_paragraphs
            if re.compile('^(\n|\s|\t|\r)+$').search(item) is None]
        speech_dict["text"] = " ".join(text_paragraphs)
        pdf_url = "https://www.bis.org" + speech_center.css(
            ".pdftitle")[0].css("a::attr(href)").extract_first()
        pdf_text = self.get_text_from_pdf(pdf_url)
        if pdf_text is not None:
            speech_dict["pdf_text"] = pdf_text
        yield speech_dict

    
    def get_speaker_gender(self, speaker_gender_phrase, speaker_name):
        """"""

        speaker_gender = None
        if speaker_gender_phrase=="Mr":
            speaker_gender = "male"
        elif speaker_gender_phrase=="Ms":
            speaker_gender = "female"
        if speaker_gender is None:
            gender_detector = gender.Detector()
            speaker_gender = gender_detector.get_gender(speaker_name[0])
        if speaker_gender in ["female", "male"]:
            return speaker_gender


    def get_speaker_title(self, title_location_phrase):
        """"""

        tokens = title_location_phrase.split(" ")
        title_info = []
        for token in tokens:
            if token[0].isupper():
                title_info.append(token)
            else:
                break
        if len(title_info)>=1:
            return  " ".join(title_info)


    def get_central_bank_name(self, title_location_phrase):
        """"""

        bank_section = [
            item for item in title_location_phrase.split(",") if "Bank " in item]
        if len(bank_section)>=1:
            if bank_section[0].strip().startswith("Bank"):
                return bank_section[0].strip()
            else:
                return self.bank_pattern.search(bank_section[0]).group("bank_name")


    def get_speech_location(self, title_location_phrase):
        """"""

        speech_city = None
        speech_country = None
        gc = geonamescache.GeonamesCache()
        potential_city = title_location_phrase.split(",")[-1].strip()
        if len(gc.get_cities_by_name(potential_city)):
            speech_city = potential_city
        places = GeoText(title_location_phrase)
        if len(places.countries)>=1:
            speech_country = places.countries[0]
        if speech_city is None and len(places.cities)>=1:
            speech_city = places.cities[0]
        if speech_country is None and speech_city is not None:
            speech_country = self.find_country_from_city(gc, speech_city)
        return speech_city, speech_country
    

    def find_country_from_city(self, gc, city):
        """"""

        country = None
        keys = [
            list(gc.get_cities_by_name(city)[index].keys())[0]
            for index in range(len(gc.get_cities_by_name(city)))]
        current_population = 0
        for i, key in enumerate(keys):
            if gc.get_cities_by_name(city)[i][key]["population"] >= current_population:
                country_code = gc.get_cities_by_name(city)[i][key]["countrycode"]
                country = gc.get_countries()[country_code]["name"]
        return country


    def get_text_from_pdf(self, pdf_url):
        """"""

        rsrcmgr = PDFResourceManager()
        retstr = io.StringIO()
        laparams = LAParams()
        f = urllib.request.urlopen(pdf_url).read()
        fp = io.BytesIO(f)
        parser = PDFParser(fp)
        doc = PDFDocument()
        parser.set_document(doc)
        doc.set_parser(parser)
        doc.initialize('')
        laparams.char_margin = 1.0
        laparams.word_margin = 1.0
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        extracted_text = ''
        for page in doc.get_pages():
            interpreter.process_page(page)
            layout = device.get_result()
            for lt_obj in layout:
                if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                    extracted_text += lt_obj.get_text()
        return extracted_text
