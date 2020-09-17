# Core Library modules
import datetime
from io import BytesIO

# Third party modules
import fitz
import magic
from PIL import Image
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
mime = magic.Magic(mime=True)


class Page(Base):
    __tablename__ = "page"
    id = Column(Integer, primary_key=True, nullable=False)
    url = Column(String, unique=True, nullable=False)
    last_checked = Column(DateTime(timezone=True), nullable=True)
    content_type = Column(String, nullable=True)
    size = Column(Integer(), nullable=True, comment="in bytes")

    def was_parsed(self):
        self.last_checked = datetime.datetime.now(datetime.timezone.utc)


class EMail(Base):
    __tablename__ = "email"
    address = Column(String, primary_key=True, nullable=False)


class File(Base):
    __tablename__ = "file"
    id = Column(Integer, primary_key=True, nullable=False)
    url = Column(String, nullable=False)
    size = Column(Integer(), nullable=False, comment="in bytes")
    last_checked = Column(DateTime(timezone=True), nullable=True)
    mime_type = Column(String, nullable=False)


class RasterImage(Base):
    __tablename__ = "image"
    id = Column(Integer, primary_key=True, nullable=False)
    url = Column(String, nullable=False)
    size = Column(Integer(), nullable=False, comment="in bytes")
    width = Column(Integer(), nullable=False, comment="in pixel")
    height = Column(Integer(), nullable=False, comment="in pixel")
    mime_type = Column(String, nullable=False)
    last_checked = Column(DateTime(timezone=True), nullable=False)


class Pdf(Base):
    __tablename__ = "pdf"
    id = Column(Integer, primary_key=True, nullable=False)
    url = Column(String, nullable=False)
    size = Column(Integer(), nullable=False, comment="in bytes")
    author = Column(String, nullable=True)
    title = Column(String, nullable=True)
    subject = Column(String, nullable=True)
    keywords = Column(String, nullable=True)
    producer = Column(String, nullable=True)
    creator = Column(String, nullable=True)
    format = Column(String, nullable=True)
    encryption = Column(String, nullable=True)
    creationDate = Column(String, nullable=True)
    modDate = Column(String, nullable=True)
    last_checked = Column(DateTime(timezone=True), nullable=False)


def initialize(engine):
    Base.metadata.create_all(engine)


def get_next_page(session):
    query = (
        session.query(Page)
        .order_by(Page.last_checked)
        .filter(Page.last_checked == None)
    )
    return query.first()


def is_in_db(session, url):
    query = session.query(Page).filter(Page.url == url)
    is_in = query.first() is not None
    return is_in


def create_file(page: Page, content):
    file_obj = File(
        url=page.url,
        size=len(content),
        mime_type=mime.from_buffer(content),
        last_checked=page.last_checked,
    )
    return file_obj


def create_image(file_obj: File, content):
    image = Image.open(BytesIO(content))
    image_obj = RasterImage(
        url=file_obj.url,
        size=file_obj.size,
        width=image.width,
        height=image.height,
        mime_type=file_obj.mime_type,
        last_checked=file_obj.last_checked,
    )
    return image_obj


def create_pdf(file_obj: File, content):
    stream = BytesIO(content)
    doc = fitz.open("x.pdf", stream)
    pdf = Pdf(
        url=file_obj.url,
        size=file_obj.size,
        last_checked=file_obj.last_checked,
        format=doc.metadata["format"],
        producer=doc.metadata["producer"],
        creator=doc.metadata["creator"],
        author=doc.metadata["author"],
        title=doc.metadata["title"],
        subject=doc.metadata["subject"],
        encryption=doc.metadata["encryption"],
        creationDate=doc.metadata["creationDate"],
        modDate=doc.metadata["modDate"],
        keywords=doc.metadata["keywords"],
    )
    return pdf
