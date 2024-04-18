#!/usr/bin/env python
"""Test the local_index"""
import pytest

import colrev.env.local_index
import colrev.env.tei_parser
import colrev.review_manager
from colrev.constants import ENTRYTYPES
from colrev.constants import Fields
from colrev.constants import LocalIndexFields
from colrev.constants import RecordState
from colrev.env.local_index_prep import prepare_record_for_indexing

# pylint: disable=line-too-long


@pytest.mark.parametrize(
    "record_dict, expected",
    [
        (
            {
                Fields.ID: "AbbasZhouDengEtAl2018",
                Fields.STATUS: RecordState.md_processed,
                Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
                Fields.METADATA_SOURCE_REPOSITORY_PATHS: "/path/to/selected_repo",
                Fields.YEAR: "2014",
                "literature_review": "yes",
                Fields.JOURNAL: "MIS Quarterly",
                Fields.AUTHOR: "Abbas, Ahmed and Zhou, Yilu and Deng, Shasha and Zhang, Pengzhu",
                Fields.TITLE: "Text Analytics to Support Sense-Making in Social Media: A Language-Action Perspective",
            },
            {
                LocalIndexFields.ID: "e44d8844c3d815a912040065ae5b9f051084b5633f110e88927094a1e331f79c",
                LocalIndexFields.CITATION_KEY: "AbbasZhouDengEtAl2018",
                Fields.COLREV_ID: "colrev_id1:|a|mis-quarterly|-|-|2014|abbas-zhou-deng-zhang|text-analytics-to-support-sense-making-in-social-media-a-language-action-perspective",
                Fields.ID: "AbbasZhouDengEtAl2018",
                Fields.STATUS: RecordState.md_processed,
                Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
                Fields.D_PROV: {
                    "literature_review": {
                        "note": "",
                        "source": "/path/to/selected_repo",
                    },
                },
                Fields.MD_PROV: {
                    Fields.YEAR: {"note": "", "source": "/path/to/selected_repo"},
                    Fields.JOURNAL: {"note": "", "source": "/path/to/selected_repo"},
                    Fields.AUTHOR: {"note": "", "source": "/path/to/selected_repo"},
                    Fields.TITLE: {"note": "", "source": "/path/to/selected_repo"},
                },
                Fields.YEAR: 2014,
                "literature_review": "yes",
                Fields.JOURNAL: "MIS Quarterly",
                Fields.AUTHOR: "Abbas, Ahmed and Zhou, Yilu and Deng, Shasha and Zhang, Pengzhu",
                Fields.TITLE: "Text Analytics to Support Sense-Making in Social Media: A Language-Action Perspective",
            },
        ),
    ],
)
def test_prepare_record_for_indexing(record_dict: dict, expected: dict, local_index) -> None:  # type: ignore

    prepare_record_for_indexing(record_dict)
    assert record_dict == expected


def test_get_year_from_toc(local_index) -> None:  # type: ignore
    """Test get_year_from_toc()"""

    with pytest.raises(
        colrev.exceptions.TOCNotAvailableException,
    ):
        record_dict = {
            Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
            Fields.VOLUME: "42",
            Fields.NUMBER: "2",
        }
        local_index.get_year_from_toc(record_dict=record_dict)

    record_dict = {
        Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
        Fields.JOURNAL: "MIS Quarterly",
        Fields.VOLUME: "42",
        Fields.NUMBER: "2",
    }
    expected = "2018"
    actual = local_index.get_year_from_toc(record_dict=record_dict)
    assert expected == actual


def test_search(local_index) -> None:  # type: ignore
    """Test search()"""

    expected = [
        colrev.record.record.Record(
            {
                Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
                Fields.ID: "AbbasZhouDengEtAl2018",
                Fields.AUTHOR: "Abbas, Ahmed and Zhou, Yilu and Deng, Shasha and Zhang, Pengzhu",
                Fields.D_PROV: {
                    Fields.DOI: {"note": "", "source": "pdfs.bib/0000000089"},
                    Fields.URL: {"note": "", "source": "DBLP.bib/001187"},
                },
                Fields.MD_PROV: {"CURATED": {"note": "", "source": "gh..."}},
                Fields.STATUS: RecordState.md_prepared,
                "curation_ID": "gh...#AbbasZhouDengEtAl2018",
                Fields.DOI: "10.25300/MISQ/2018/13239",
                Fields.JOURNAL: "MIS Quarterly",
                Fields.LANGUAGE: "eng",
                Fields.NUMBER: "2",
                Fields.PAGES: "427--464",
                Fields.TITLE: "Text Analytics to Support Sense-Making in Social Media: A Language-Action Perspective",
                Fields.URL: "https://misq.umn.edu/skin/frontend/default/misq/pdf/appendices/2018/V42I2Appendices/04_13239_RA_AbbasiZhou.pdf",
                Fields.VOLUME: "42",
                Fields.YEAR: "2018",
            }
        )
    ]
    actual = local_index.search(query="title LIKE '%social media%'")
    assert expected == actual

    expected = [
        colrev.record.record.Record(
            {
                Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
                Fields.ID: "AlaviLeidner2001",
                Fields.AUTHOR: "Alavi, Maryam and Leidner, Dorothy E.",
                Fields.D_PROV: {
                    Fields.DOI: {"note": "", "source": "CROSSREF.bib/000516"},
                    Fields.URL: {"note": "", "source": "DBLP.bib/000528"},
                    "literature_review": {"note": "", "source": "CURATED:gh..."},
                },
                Fields.MD_PROV: {"CURATED": {"note": "", "source": "gh..."}},
                Fields.STATUS: RecordState.md_prepared,
                "curation_ID": "gh...#AlaviLeidner2001",
                Fields.DOI: "10.2307/3250961",
                Fields.JOURNAL: "MIS Quarterly",
                "literature_review": "yes",
                Fields.LANGUAGE: "eng",
                Fields.NUMBER: "1",
                Fields.TITLE: "Review: Knowledge Management and Knowledge Management Systems: Conceptual Foundations and Research Issues",
                Fields.URL: "https://www.doi.org/10.2307/3250961",
                Fields.VOLUME: "25",
                Fields.YEAR: "2001",
            }
        )
    ]
    actual = local_index.search(
        query="title LIKE '%Knowledge Management and Knowledge Management Systems%'"
    )
    assert expected == actual


def test_get_fields_to_remove(local_index) -> None:  # type: ignore
    """Test get_fields_to_remove()"""

    record_dict = {
        Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
        Fields.JOURNAL: "Communications of the Association for Information Systems",
        Fields.YEAR: "2021",
        Fields.VOLUME: "48",
        Fields.NUMBER: "2",
    }
    expected = [Fields.NUMBER]
    actual = local_index.get_fields_to_remove(record_dict=record_dict)
    assert expected == actual

    record_dict = {
        Fields.ENTRYTYPE: ENTRYTYPES.INPROCEEDINGS,
        "booktitle": "Communications of the Association for Information Systems",
        Fields.YEAR: "2021",
        Fields.VOLUME: "48",
        Fields.NUMBER: "2",
    }
    expected = []
    actual = local_index.get_fields_to_remove(record_dict=record_dict)
    assert expected == actual


def test_retrieve_from_toc(local_index) -> None:  # type: ignore
    """Test retrieve_from_toc()"""

    record_dict = {
        Fields.ID: "AbbasZhouDengEtAl2018",
        Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
        Fields.AUTHOR: "Abbas, Ahmed and Zhou, Yilu and Deng, Shasha and Zhang, Pengzhu",
        Fields.JOURNAL: "MIS Quarterly",
        Fields.LANGUAGE: "eng",
        Fields.NUMBER: "2",
        Fields.PAGES: "427-64",
        "curation_ID": "gh...#AbbasZhouDengEtAl2018",
        Fields.TITLE: "Text Analytics to Support Sense-Making in Social Media: A Language Perspective",
        Fields.VOLUME: "42",
        Fields.YEAR: "2018",
    }
    record = colrev.record.record.Record(record_dict)
    expected_dict = {
        Fields.ID: "AbbasZhouDengEtAl2018",
        Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
        Fields.STATUS: RecordState.md_prepared,
        Fields.MD_PROV: {"CURATED": {"source": "gh...", "note": ""}},
        Fields.D_PROV: {
            Fields.DOI: {"source": "pdfs.bib/0000000089", "note": ""},
            Fields.URL: {"source": "DBLP.bib/001187", "note": ""},
        },
        Fields.DOI: "10.25300/MISQ/2018/13239",
        Fields.JOURNAL: "MIS Quarterly",
        Fields.TITLE: "Text Analytics to Support Sense-Making in Social Media: A Language-Action Perspective",
        Fields.YEAR: "2018",
        Fields.VOLUME: "42",
        Fields.NUMBER: "2",
        Fields.PAGES: "427--464",
        Fields.URL: "https://misq.umn.edu/skin/frontend/default/misq/pdf/appendices/2018/V42I2Appendices/04_13239_RA_AbbasiZhou.pdf",
        Fields.LANGUAGE: "eng",
        Fields.AUTHOR: "Abbas, Ahmed and Zhou, Yilu and Deng, Shasha and Zhang, Pengzhu",
        "curation_ID": "gh...#AbbasZhouDengEtAl2018",
    }
    actual = local_index.retrieve_from_toc(record)
    expected = colrev.record.record.Record(expected_dict)
    assert expected == actual


def test_retrieve_based_on_colrev_pdf_id(local_index) -> None:  # type: ignore
    """Test retrieve_based_on_colrev_pdf_id()"""

    colrev_pdf_id = "cpid2:fffffffffcffffffe007ffffc0020003e0f20007fffffffff000000fff8001fffffc3fffffe007ffffc003fffe00007ffffffffff800001ff800001ff80003fff920725ff800001ff800001ff800001ff84041fff81fffffffffffffe000afffe0018007efff8007e2bd8007efff8007e00fffffffffffffffffffffffffffff"
    expected = colrev.record.record.Record(
        {
            Fields.ID: "AbbasiAlbrechtVanceEtAl2012",
            Fields.ENTRYTYPE: ENTRYTYPES.ARTICLE,
            Fields.STATUS: RecordState.md_prepared,
            Fields.MD_PROV: {"CURATED": {"source": "gh...", "note": ""}},
            Fields.D_PROV: {
                Fields.PDF_ID: {"source": "file|pdf_hash", "note": ""},
                Fields.FILE: {"source": "pdfs.bib/0000001378", "note": ""},
                Fields.DBLP_KEY: {"source": "DBLP.bib/000869", "note": ""},
                Fields.URL: {"source": "DBLP.bib/000869", "note": ""},
            },
            Fields.PDF_ID: "cpid2:fffffffffcffffffe007ffffc0020003e0f20007fffffffff000000fff8001fffffc3fffffe007ffffc003fffe00007ffffffffff800001ff800001ff80003fff920725ff800001ff800001ff800001ff84041fff81fffffffffffffe000afffe0018007efff8007e2bd8007efff8007e00fffffffffffffffffffffffffffff",
            Fields.DBLP_KEY: "https://dblp.org/rec/journals/misq/AbbasiAVH12",
            Fields.JOURNAL: "MIS Quarterly",
            Fields.TITLE: "MetaFraud - A Meta-Learning Framework for Detecting Financial Fraud",
            Fields.YEAR: "2012",
            Fields.VOLUME: "36",
            Fields.NUMBER: "4",
            Fields.URL: "http://misq.org/metafraud-a-meta-learning-framework-for-detecting-financial-fraud.html",
            Fields.LANGUAGE: "eng",
            Fields.AUTHOR: "Abbasi, Ahmed and Albrecht, Conan and Vance, Anthony and Hansen, James",
            "curation_ID": "gh...#AbbasiAlbrechtVanceEtAl2012",
        }
    )
    actual = local_index.retrieve_based_on_colrev_pdf_id(colrev_pdf_id=colrev_pdf_id)
    assert expected == actual


# next tests: index_tei:
# we could leave the file field for WagnerLukyanenkoParEtAl2022
# but if the PDF does not exist, the field is removed
# del record_dict[Fields.FILE]
# and the index_tei immediately returns.

# def method(): # pragma: no cover
